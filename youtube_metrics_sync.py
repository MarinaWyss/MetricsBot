#!/usr/bin/env python3
"""
YouTube Metrics Fetcher
=======================
Pulls comprehensive metrics from YouTube Data API v3 + YouTube Analytics API
and outputs a JSON file. The scheduled task reads this JSON and writes to
Notion using its built-in connector — no Notion API key needed.

Setup:
  1. Create a Google Cloud project at https://console.cloud.google.com
  2. Enable "YouTube Data API v3" and "YouTube Analytics API"
  3. Create OAuth 2.0 credentials (Desktop app type)
  4. Download the credentials JSON and save as client_secret.json
  5. Set your environment variables (see below)
  6. Run once manually to complete OAuth flow, then schedule daily

Environment Variables:
  YOUTUBE_CLIENT_SECRET_PATH  - Path to your OAuth client_secret.json
  YOUTUBE_TOKEN_PATH          - Path to store/read the OAuth token (token.json)
  YOUTUBE_CHANNEL_ID          - Your YouTube channel ID (optional, auto-detected)

Output:
  youtube_metrics.json — array of video objects with all metrics
"""

import os
import sys
import json
import re
import logging
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
except ImportError:
    print("Installing required packages...")
    import subprocess
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--break-system-packages", "-q",
        "google-api-python-client",
        "google-auth-oauthlib",
        "google-auth-httplib2",
    ])
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
]

CLIENT_SECRET_PATH = os.environ.get("YOUTUBE_CLIENT_SECRET_PATH", "client_secret_619912389167-o8ir10s43r5cvsup8k82s9jmcoov9jgp.apps.googleusercontent.com.json")
TOKEN_PATH = os.environ.get("YOUTUBE_TOKEN_PATH", "token.json")
CHANNEL_ID = os.environ.get("YOUTUBE_CHANNEL_ID", "")
OUTPUT_PATH = os.environ.get("YOUTUBE_METRICS_OUTPUT", "youtube_metrics.json")


# ===================================================================
# AUTH
# ===================================================================
def get_youtube_credentials() -> Credentials:
    """Authenticate with Google OAuth 2.0, caching the token for reuse."""
    creds = None
    token_path = Path(TOKEN_PATH)

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log.info("Refreshing expired OAuth token...")
            creds.refresh(Request())
        else:
            if not Path(CLIENT_SECRET_PATH).exists():
                log.error(
                    f"OAuth client secret not found at {CLIENT_SECRET_PATH}. "
                    "Download it from Google Cloud Console → APIs & Services → Credentials."
                )
                sys.exit(1)
            log.info("Starting OAuth flow — a browser window will open...")
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())
        log.info(f"Token saved to {TOKEN_PATH}")

    return creds


# ===================================================================
# YOUTUBE DATA API — Video metadata & public stats
# ===================================================================
def get_channel_id(youtube) -> str:
    if CHANNEL_ID:
        return CHANNEL_ID
    resp = youtube.channels().list(part="id", mine=True).execute()
    return resp["items"][0]["id"]


def parse_duration(iso_duration: str) -> float:
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso_duration or "")
    if not match:
        return 0.0
    h, m, s = (int(g) if g else 0 for g in match.groups())
    return round(h * 60 + m + s / 60, 2)


def fetch_all_videos(youtube, channel_id: str) -> list[dict]:
    log.info("Fetching video list from channel...")
    video_ids = []
    next_page = None

    while True:
        resp = youtube.search().list(
            part="id", channelId=channel_id, type="video",
            maxResults=50, order="date", pageToken=next_page,
        ).execute()
        video_ids.extend(item["id"]["videoId"] for item in resp.get("items", []))
        next_page = resp.get("nextPageToken")
        if not next_page:
            break

    log.info(f"Found {len(video_ids)} videos. Fetching details...")

    videos = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        resp = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch),
        ).execute()
        for item in resp.get("items", []):
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})
            videos.append({
                "video_id": item["id"],
                "title": snippet["title"],
                "published_at": snippet["publishedAt"],
                "duration_min": parse_duration(content.get("duration", "")),
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
            })

    # Filter out Shorts (videos ≤ 1 minute)
    long_videos = [v for v in videos if v["duration_min"] > 1.0]
    shorts_count = len(videos) - len(long_videos)
    if shorts_count:
        log.info(f"Filtered out {shorts_count} Shorts (≤ 1 min). Keeping {len(long_videos)} videos.")
    else:
        log.info(f"Fetched details for {len(long_videos)} videos (no Shorts found).")
    return long_videos


# ===================================================================
# YOUTUBE ANALYTICS API — Studio-level metrics per video
# ===================================================================
def fetch_analytics_for_video(yt_analytics, video_id: str, published_date: str) -> dict:
    start_date = published_date[:10]
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    metrics = (
        "views,estimatedMinutesWatched,averageViewDuration,"
        "averageViewPercentage,shares,subscribersGained,subscribersLost,"
        "annotationImpressions,cardImpressions"
    )
    try:
        resp = yt_analytics.reports().query(
            ids="channel==MINE", startDate=start_date, endDate=end_date,
            metrics=metrics, filters=f"video=={video_id}", dimensions="",
        ).execute()
        row = resp.get("rows", [[]])[0]
        if not row:
            return {}
        return {
            "watch_time_hrs": round(float(row[1]) / 60, 2) if len(row) > 1 else 0,
            "avg_view_duration_min": round(float(row[2]) / 60, 2) if len(row) > 2 else 0,
            "avg_pct_viewed": round(float(row[3]), 2) if len(row) > 3 else 0,
            "shares": int(row[4]) if len(row) > 4 else 0,
            "subs_gained": int(row[5]) if len(row) > 5 else 0,
            "subs_lost": int(row[6]) if len(row) > 6 else 0,
        }
    except Exception as e:
        log.warning(f"Analytics failed for {video_id}: {e}")
        return {}


def fetch_traffic_sources(yt_analytics, video_id: str, published_date: str) -> dict:
    start_date = published_date[:10]
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        resp = yt_analytics.reports().query(
            ids="channel==MINE", startDate=start_date, endDate=end_date,
            metrics="views", filters=f"video=={video_id}",
            dimensions="insightTrafficSourceType",
        ).execute()
        traffic = {}
        total_views = 0
        for row in resp.get("rows", []):
            traffic[row[0]] = int(row[1])
            total_views += int(row[1])
        if total_views > 0:
            return {
                "search_pct": round(traffic.get("YT_SEARCH", 0) / total_views * 100, 1),
                "suggested_pct": round(traffic.get("RELATED_VIDEO", 0) / total_views * 100, 1),
                "browse_pct": round(traffic.get("SUBSCRIBER", 0) / total_views * 100, 1),
                "external_pct": round(traffic.get("EXT_URL", 0) / total_views * 100, 1),
            }
        return {}
    except Exception as e:
        log.warning(f"Traffic source query failed for {video_id}: {e}")
        return {}


def fetch_revenue(yt_analytics, video_id: str, published_date: str) -> dict:
    start_date = published_date[:10]
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        resp = yt_analytics.reports().query(
            ids="channel==MINE", startDate=start_date, endDate=end_date,
            metrics="estimatedRevenue,estimatedAdRevenue,estimatedRedPartnerRevenue",
            filters=f"video=={video_id}",
        ).execute()
        row = resp.get("rows", [[]])[0]
        if not row:
            return {}
        return {"revenue": round(float(row[0]), 2) if len(row) > 0 else 0}
    except Exception as e:
        log.debug(f"Revenue query failed for {video_id}: {e}")
        return {}


def fetch_demographics(yt_analytics, video_id: str, published_date: str) -> dict:
    start_date = published_date[:10]
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        resp = yt_analytics.reports().query(
            ids="channel==MINE", startDate=start_date, endDate=end_date,
            metrics="viewerPercentage", dimensions="gender",
            filters=f"video=={video_id}",
        ).execute()
        gender_data = {"female_pct": 0}
        for row in resp.get("rows", []):
            if row[0] == "female":
                gender_data["female_pct"] = round(float(row[1]), 1)
        return gender_data
    except Exception as e:
        log.warning(f"Demographics query failed for {video_id}: {e}")
        return {}


COUNTRY_NAMES = {
    "US": "United States", "IN": "India", "GB": "United Kingdom",
    "CA": "Canada", "AU": "Australia", "DE": "Germany", "FR": "France",
    "BR": "Brazil", "MX": "Mexico", "JP": "Japan", "KR": "South Korea",
    "ID": "Indonesia", "PH": "Philippines", "PK": "Pakistan",
    "NG": "Nigeria", "BD": "Bangladesh", "RU": "Russia", "IT": "Italy",
    "ES": "Spain", "NL": "Netherlands", "TR": "Turkey", "TH": "Thailand",
    "VN": "Vietnam", "PL": "Poland", "SA": "Saudi Arabia", "EG": "Egypt",
    "ZA": "South Africa", "CO": "Colombia", "AR": "Argentina",
    "MY": "Malaysia", "SG": "Singapore", "SE": "Sweden", "NO": "Norway",
    "DK": "Denmark", "FI": "Finland", "IE": "Ireland", "NZ": "New Zealand",
    "CL": "Chile", "PE": "Peru", "RO": "Romania", "UA": "Ukraine",
    "CZ": "Czech Republic", "PT": "Portugal", "HK": "Hong Kong",
    "TW": "Taiwan", "IL": "Israel", "AE": "UAE", "KE": "Kenya",
    "GH": "Ghana", "MA": "Morocco", "AT": "Austria", "CH": "Switzerland",
    "BE": "Belgium", "HU": "Hungary", "GR": "Greece", "RS": "Serbia",
}


def fetch_top_countries(yt_analytics, video_id: str, published_date: str) -> dict:
    start_date = published_date[:10]
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        resp = yt_analytics.reports().query(
            ids="channel==MINE", startDate=start_date, endDate=end_date,
            metrics="views", dimensions="country", filters=f"video=={video_id}",
            sort="-views", maxResults=3,
        ).execute()
        rows = resp.get("rows", [])
        if not rows:
            return {}
        total_views = sum(int(r[1]) for r in rows)
        countries = {}
        for i, row in enumerate(rows[:3], 1):
            code = row[0]
            views = int(row[1])
            pct = round(views / total_views * 100, 1) if total_views > 0 else 0
            countries[f"country_{i}"] = f"{COUNTRY_NAMES.get(code, code)} {pct}%"
        return countries
    except Exception as e:
        log.warning(f"Country query failed for {video_id}: {e}")
        return {}


# ===================================================================
# MAIN — Fetch everything and output JSON
# ===================================================================
def main():
    log.info("=" * 60)
    log.info("YouTube Metrics Fetcher")
    log.info("=" * 60)

    creds = get_youtube_credentials()
    youtube = build("youtube", "v3", credentials=creds)
    yt_analytics = build("youtubeAnalytics", "v2", credentials=creds)

    channel_id = get_channel_id(youtube)
    log.info(f"Channel ID: {channel_id}")

    videos = fetch_all_videos(youtube, channel_id)
    if not videos:
        log.warning("No videos found on channel.")
        return

    log.info(f"\nFetching analytics for {len(videos)} videos...")
    results = []

    for i, video in enumerate(videos, 1):
        try:
            log.info(f"[{i}/{len(videos)}] {video['title'][:60]}...")

            analytics = fetch_analytics_for_video(yt_analytics, video["video_id"], video["published_at"])
            traffic = fetch_traffic_sources(yt_analytics, video["video_id"], video["published_at"])
            revenue = fetch_revenue(yt_analytics, video["video_id"], video["published_at"])
            demographics = fetch_demographics(yt_analytics, video["video_id"], video["published_at"])
            countries = fetch_top_countries(yt_analytics, video["video_id"], video["published_at"])

            # Flatten into a single record
            record = {
                "video_id": video["video_id"],
                "title": video["title"],
                "published_at": video["published_at"][:10],
                "duration_min": video["duration_min"],
                "views": video["views"],
                "likes": video["likes"],
                "comments": video["comments"],
                # Analytics
                "watch_time_hrs": analytics.get("watch_time_hrs"),
                "avg_view_duration_min": analytics.get("avg_view_duration_min"),
                "avg_pct_viewed": analytics.get("avg_pct_viewed"),
                "shares": analytics.get("shares"),
                "subs_gained": analytics.get("subs_gained"),
                "subs_lost": analytics.get("subs_lost"),
                # Traffic
                "search_pct": traffic.get("search_pct"),
                "suggested_pct": traffic.get("suggested_pct"),
                "browse_pct": traffic.get("browse_pct"),
                "external_pct": traffic.get("external_pct"),
                # Revenue
                "revenue": revenue.get("revenue"),
                "rpm": round(revenue["revenue"] / video["views"] * 1000, 2)
                    if revenue.get("revenue") and video["views"] > 0 else None,
                # Demographics
                "female_pct": demographics.get("female_pct"),
                # Countries
                "country_1": countries.get("country_1"),
                "country_2": countries.get("country_2"),
                "country_3": countries.get("country_3"),
            }
            results.append(record)

        except Exception as e:
            log.error(f"  Error processing {video['video_id']}: {e}")

    # Write JSON output
    output_path = Path(OUTPUT_PATH)
    output_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")

    log.info(f"\n{'=' * 60}")
    log.info(f"Done! {len(results)} videos written to {OUTPUT_PATH}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
