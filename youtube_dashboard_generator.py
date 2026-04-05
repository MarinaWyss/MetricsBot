#!/usr/bin/env python3
"""
YouTube Analytics Dashboard Generator
======================================
Fetches time-series views, comments, retention curves, and transcripts
from the YouTube APIs, analyzes them with Claude, and generates a
self-contained HTML dashboard.

Requires the same Google OAuth setup as youtube_metrics_sync.py, plus:
  ANTHROPIC_API_KEY  — Your Anthropic API key for Claude analysis

Usage:
  python youtube_dashboard_generator.py
  python youtube_dashboard_generator.py --output /path/to/dashboard.html
"""

import os
import sys
import json
import re
import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    import requests
except ImportError:
    import subprocess
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--break-system-packages", "-q",
        "google-api-python-client", "google-auth-oauthlib",
        "google-auth-httplib2", "requests",
    ])
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    import requests

try:
    from youtube_transcript_api import YouTubeTranscriptApi
except ImportError:
    import subprocess
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--break-system-packages", "-q",
        "youtube-transcript-api",
    ])
    from youtube_transcript_api import YouTubeTranscriptApi

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config (reuses same env vars as sync script)
# ---------------------------------------------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
]
CLIENT_SECRET_PATH = os.environ.get("YOUTUBE_CLIENT_SECRET_PATH", "client_secret_619912389167-o8ir10s43r5cvsup8k82s9jmcoov9jgp.apps.googleusercontent.com.json")
TOKEN_PATH = os.environ.get("YOUTUBE_TOKEN_PATH", "token.json")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CHANNEL_ID = os.environ.get("YOUTUBE_CHANNEL_ID", "")
TREND_DAYS = 90


# ===================================================================
# AUTH (same as sync script)
# ===================================================================
def get_youtube_credentials() -> Credentials:
    creds = None
    token_path = Path(TOKEN_PATH)
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())
    return creds


def get_channel_id(youtube) -> str:
    if CHANNEL_ID:
        return CHANNEL_ID
    resp = youtube.channels().list(part="id", mine=True).execute()
    return resp["items"][0]["id"]


# ===================================================================
# DATA FETCHING
# ===================================================================
def fetch_all_video_ids(youtube, channel_id: str) -> list[dict]:
    """Get all videos with basic metadata."""
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

    # Enrich with snippet + stats
    videos = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        resp = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch),
        ).execute()
        for item in resp.get("items", []):
            s = item["snippet"]
            st = item.get("statistics", {})
            dur_match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?",
                                 item.get("contentDetails", {}).get("duration", ""))
            dur_min = 0
            if dur_match:
                h, m, sec = (int(g) if g else 0 for g in dur_match.groups())
                dur_min = round(h * 60 + m + sec / 60, 2)

            videos.append({
                "video_id": item["id"],
                "title": s["title"],
                "published_at": s["publishedAt"][:10],
                "views": int(st.get("viewCount", 0)),
                "likes": int(st.get("likeCount", 0)),
                "comments_count": int(st.get("commentCount", 0)),
                "duration_min": dur_min,
            })
    # Filter out Shorts (videos ≤ 1 minute)
    long_videos = [v for v in videos if v["duration_min"] > 1.0]
    shorts_count = len(videos) - len(long_videos)
    if shorts_count:
        log.info(f"Filtered out {shorts_count} Shorts (≤ 1 min). Keeping {len(long_videos)} videos.")
    return long_videos


def fetch_daily_views(yt_analytics, video_id: str, published_date: str) -> list[dict]:
    """Fetch daily view counts for a video over the trend window."""
    start = max(
        published_date,
        (datetime.now(timezone.utc) - timedelta(days=TREND_DAYS)).strftime("%Y-%m-%d"),
    )
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        resp = yt_analytics.reports().query(
            ids="channel==MINE",
            startDate=start, endDate=end,
            metrics="views,estimatedMinutesWatched",
            dimensions="day",
            filters=f"video=={video_id}",
            sort="day",
        ).execute()
        return [{"date": r[0], "views": int(r[1]), "watch_min": round(float(r[2]), 1)}
                for r in resp.get("rows", [])]
    except Exception as e:
        log.warning(f"Daily views failed for {video_id}: {e}")
        return []


def fetch_retention_curve(yt_analytics, video_id: str, published_date: str) -> list[dict]:
    """Fetch audience retention curve (100 data points across video duration)."""
    start = published_date
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        resp = yt_analytics.reports().query(
            ids="channel==MINE",
            startDate=start, endDate=end,
            metrics="audienceWatchRatio,relativeRetentionPerformance",
            dimensions="elapsedVideoTimeRatio",
            filters=f"video=={video_id}",
            sort="elapsedVideoTimeRatio",
        ).execute()
        return [
            {
                "position": round(float(r[0]), 4),
                "retention": round(float(r[1]) * 100, 1),
                "relative": round(float(r[2]), 2),
            }
            for r in resp.get("rows", [])
        ]
    except Exception as e:
        log.warning(f"Retention curve failed for {video_id}: {e}")
        return []


def fetch_comments(youtube, video_id: str, max_comments: int = 100) -> list[str]:
    """Fetch top-level comments for a video."""
    comments = []
    try:
        next_page = None
        while len(comments) < max_comments:
            resp = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=min(100, max_comments - len(comments)),
                order="relevance",
                textFormat="plainText",
                pageToken=next_page,
            ).execute()
            for item in resp.get("items", []):
                text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                comments.append(text)
            next_page = resp.get("nextPageToken")
            if not next_page:
                break
    except Exception as e:
        log.warning(f"Comments failed for {video_id}: {e}")
    return comments


def fetch_transcript(video_id: str) -> list[dict]:
    """Fetch video transcript/captions using youtube-transcript-api."""
    try:
        # youtube-transcript-api v1.x: instance-based API
        ytt = YouTubeTranscriptApi()
        fetched = ytt.fetch(video_id, languages=["en"])
        return [
            {
                "start": round(snippet.start, 1),
                "duration": round(snippet.duration, 1),
                "text": snippet.text,
            }
            for snippet in fetched
        ]
    except Exception as e:
        log.warning(f"Transcript failed for {video_id}: {e}")
        return []


# ===================================================================
# CLAUDE ANALYSIS
# ===================================================================
def call_claude(prompt: str, max_tokens: int = 1500) -> str:
    """Call Claude API for analysis."""
    if not ANTHROPIC_API_KEY:
        return "(Anthropic API key not set — skipping analysis)"
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"]
    except Exception as e:
        log.warning(f"Claude API call failed: {e}")
        return f"(Analysis unavailable: {e})"


def summarize_comments(video_title: str, comments: list[str]) -> str:
    if not comments:
        return "No comments available."
    sample = comments[:80]
    comments_text = "\n".join(f"- {c[:300]}" for c in sample)
    prompt = f"""Analyze these YouTube comments for the video "{video_title}".

Identify:
1. **Top 3-5 recurring themes** (what are people talking about most?)
2. **Overall sentiment** (positive/negative/mixed, with rough percentages)
3. **Notable feedback** — any specific constructive criticism or praise that stands out
4. **Content requests** — are viewers asking for follow-up videos or specific topics?

Keep your analysis concise (under 200 words). Use short bullet points.

Comments:
{comments_text}"""
    return call_claude(prompt)


def analyze_retention_vs_transcript(
    video_title: str, duration_min: float,
    retention: list[dict], transcript: list[dict]
) -> str:
    if not retention or not transcript:
        return "Retention or transcript data unavailable."

    # Build a simplified retention summary — find notable drops and spikes
    retention_summary = []
    for i, point in enumerate(retention):
        if i == 0:
            continue
        prev = retention[i - 1]["retention"]
        curr = point["retention"]
        diff = curr - prev
        if abs(diff) > 3:  # Notable change of >3%
            timestamp_sec = point["position"] * duration_min * 60
            mins = int(timestamp_sec // 60)
            secs = int(timestamp_sec % 60)
            direction = "DROP" if diff < 0 else "SPIKE"
            retention_summary.append(
                f"{direction} of {abs(diff):.1f}% at {mins}:{secs:02d} "
                f"(position {point['position']:.0%}, retention {curr:.1f}%)"
            )

    # Build transcript with timestamps
    transcript_text = ""
    for seg in transcript:
        mins = int(seg["start"] // 60)
        secs = int(seg["start"] % 60)
        transcript_text += f"[{mins}:{secs:02d}] {seg['text']}\n"

    # Truncate if too long
    if len(transcript_text) > 6000:
        transcript_text = transcript_text[:6000] + "\n[...truncated...]"

    retention_text = "\n".join(retention_summary) if retention_summary else "No major drops or spikes detected."

    prompt = f"""Analyze this YouTube video's audience retention curve against its transcript.

Video: "{video_title}" ({duration_min:.1f} min)

RETENTION EVENTS (notable drops/spikes in viewership):
{retention_text}

TRANSCRIPT:
{transcript_text}

For each major retention drop, explain what was happening in the video at that moment and why viewers likely left. For each spike, explain what likely re-engaged viewers.

Then provide:
1. **Top 3 moments that lost viewers** — what was being said/done and why it's likely boring
2. **Top 3 moments that held/gained viewers** — what was engaging
3. **Actionable scripting advice** — 2-3 specific tips for improving retention in future videos

Keep it concise and actionable (under 300 words)."""
    return call_claude(prompt, max_tokens=2000)


# ===================================================================
# SPIKE DETECTION
# ===================================================================
def detect_spikes(daily_views: list[dict]) -> list[dict]:
    """Detect days where views significantly exceed the rolling average."""
    if len(daily_views) < 14:
        return []
    spikes = []
    for i in range(7, len(daily_views)):
        window = daily_views[max(0, i-14):i]
        avg = sum(d["views"] for d in window) / len(window) if window else 1
        current = daily_views[i]["views"]
        if avg > 0 and current > avg * 2.5 and current > 20:
            spikes.append({
                "date": daily_views[i]["date"],
                "views": current,
                "avg": round(avg, 1),
                "multiplier": round(current / avg, 1),
            })
    return spikes


def compute_momentum_score(daily_views: list[dict]) -> float:
    """Score how much a video is accelerating. Positive = taking off."""
    if len(daily_views) < 14:
        return 0.0
    recent_7 = daily_views[-7:]
    prior_7 = daily_views[-14:-7]
    recent_avg = sum(d["views"] for d in recent_7) / 7
    prior_avg = sum(d["views"] for d in prior_7) / 7
    if prior_avg <= 0:
        return recent_avg
    return round((recent_avg - prior_avg) / prior_avg * 100, 1)


# ===================================================================
# HTML DASHBOARD GENERATION
# ===================================================================
def generate_dashboard_html(videos_data: list[dict], generated_at: str) -> str:
    """Generate a self-contained HTML dashboard with embedded data."""
    data_json = json.dumps(videos_data, indent=None, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>YouTube Analytics Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0f0f0f; --surface: #1a1a1a; --surface2: #252525;
    --border: #333; --text: #f1f1f1; --text2: #aaa;
    --red: #ff4444; --green: #00c853; --blue: #448aff;
    --yellow: #ffab00; --purple: #aa66cc;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; }}
  h1 {{ font-size: 24px; margin-bottom: 4px; }}
  .subtitle {{ color: var(--text2); font-size: 13px; margin-bottom: 24px; }}
  .grid {{ display: grid; grid-template-columns: 340px 1fr; gap: 20px; height: calc(100vh - 100px); }}
  .sidebar {{ overflow-y: auto; border-right: 1px solid var(--border); padding-right: 16px; }}
  .main {{ overflow-y: auto; padding-left: 4px; }}

  /* Sidebar video cards */
  .video-card {{
    background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
    padding: 12px; margin-bottom: 8px; cursor: pointer; transition: border-color 0.2s;
  }}
  .video-card:hover {{ border-color: var(--blue); }}
  .video-card.active {{ border-color: var(--blue); background: var(--surface2); }}
  .video-card .title {{ font-size: 14px; font-weight: 600; margin-bottom: 6px; line-height: 1.3;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
  .video-card .meta {{ font-size: 12px; color: var(--text2); display: flex; gap: 12px; margin-bottom: 6px; }}
  .video-card .momentum {{ font-size: 12px; font-weight: 600; padding: 2px 8px; border-radius: 4px; display: inline-block; }}
  .momentum.hot {{ background: rgba(255,68,68,0.2); color: var(--red); }}
  .momentum.warm {{ background: rgba(255,171,0,0.2); color: var(--yellow); }}
  .momentum.steady {{ background: rgba(170,170,170,0.15); color: var(--text2); }}
  .momentum.rising {{ background: rgba(0,200,83,0.2); color: var(--green); }}
  .spike-badge {{ font-size: 11px; background: rgba(255,68,68,0.15); color: var(--red); padding: 1px 6px; border-radius: 3px; margin-left: 6px; }}

  /* Sort controls */
  .sort-bar {{ display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; }}
  .sort-btn {{ background: var(--surface); border: 1px solid var(--border); color: var(--text2);
    padding: 4px 10px; border-radius: 4px; font-size: 12px; cursor: pointer; }}
  .sort-btn.active {{ border-color: var(--blue); color: var(--blue); }}

  /* Main panels */
  .panel {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin-bottom: 16px; }}
  .panel h2 {{ font-size: 18px; margin-bottom: 12px; }}
  .panel h3 {{ font-size: 15px; color: var(--text2); margin-bottom: 8px; margin-top: 16px; }}
  .chart-container {{ position: relative; height: 280px; }}
  .analysis {{ font-size: 13px; line-height: 1.7; white-space: pre-wrap; color: var(--text); }}
  .analysis strong {{ color: var(--blue); }}
  .stats-row {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 16px; }}
  .stat-box {{ background: var(--surface2); border-radius: 6px; padding: 12px 16px; min-width: 120px; }}
  .stat-box .label {{ font-size: 11px; color: var(--text2); text-transform: uppercase; letter-spacing: 0.5px; }}
  .stat-box .value {{ font-size: 22px; font-weight: 700; margin-top: 2px; }}
  .no-data {{ color: var(--text2); font-style: italic; padding: 40px; text-align: center; }}

  .search-box {{ width: 100%; padding: 8px 12px; background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; color: var(--text); font-size: 13px; margin-bottom: 12px; outline: none; }}
  .search-box:focus {{ border-color: var(--blue); }}
  .search-box::placeholder {{ color: #555; }}
</style>
</head>
<body>
<h1>YouTube Analytics Dashboard</h1>
<p class="subtitle">Generated {generated_at} &middot; {TREND_DAYS}-day trend window</p>
<div class="grid">
  <div class="sidebar">
    <input type="text" class="search-box" id="search" placeholder="Search videos..." oninput="filterVideos()">
    <div class="sort-bar">
      <button class="sort-btn active" onclick="sortBy('momentum')">Momentum</button>
      <button class="sort-btn" onclick="sortBy('views')">Views</button>
      <button class="sort-btn" onclick="sortBy('recent')">Recent</button>
      <button class="sort-btn" onclick="sortBy('spikes')">Spikes</button>
    </div>
    <div id="video-list"></div>
  </div>
  <div class="main" id="main-content">
    <div class="no-data">Select a video from the sidebar to view analytics</div>
  </div>
</div>

<script>
const VIDEOS = {data_json};
let currentSort = 'momentum';
let selectedVideoId = null;
let trendChart = null;
let retentionChart = null;

function fmt(n) {{ return n >= 1000000 ? (n/1000000).toFixed(1)+'M' : n >= 1000 ? (n/1000).toFixed(1)+'K' : n.toString(); }}

function momentumLabel(score) {{
  if (score > 100) return ['HOT — Taking off!', 'hot'];
  if (score > 30) return ['Rising', 'rising'];
  if (score > -10) return ['Steady', 'steady'];
  return ['Cooling', 'warm'];
}}

function sortBy(key) {{
  currentSort = key;
  document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');
  renderVideoList();
}}

function filterVideos() {{
  renderVideoList();
}}

function getSortedVideos() {{
  const q = document.getElementById('search').value.toLowerCase();
  let vids = VIDEOS.filter(v => v.title.toLowerCase().includes(q));
  switch(currentSort) {{
    case 'momentum': vids.sort((a,b) => b.momentum - a.momentum); break;
    case 'views': vids.sort((a,b) => b.views - a.views); break;
    case 'recent': vids.sort((a,b) => b.published_at.localeCompare(a.published_at)); break;
    case 'spikes': vids.sort((a,b) => (b.spikes?.length||0) - (a.spikes?.length||0)); break;
  }}
  return vids;
}}

function renderVideoList() {{
  const vids = getSortedVideos();
  const container = document.getElementById('video-list');
  container.innerHTML = vids.map(v => {{
    const [mLabel, mClass] = momentumLabel(v.momentum);
    const spikeCount = v.spikes?.length || 0;
    return `<div class="video-card ${{v.video_id === selectedVideoId ? 'active' : ''}}" onclick="selectVideo('${{v.video_id}}')">
      <div class="title">${{v.title}}</div>
      <div class="meta">
        <span>${{v.published_at}}</span>
        <span>${{fmt(v.views)}} views</span>
        <span>${{v.duration_min}}m</span>
      </div>
      <span class="momentum ${{mClass}}">${{mLabel}} (${{v.momentum > 0 ? '+' : ''}}${{v.momentum}}%)</span>
      ${{spikeCount > 0 ? `<span class="spike-badge">${{spikeCount}} spike${{spikeCount>1?'s':''}}</span>` : ''}}
    </div>`;
  }}).join('');
}}

function selectVideo(videoId) {{
  selectedVideoId = videoId;
  const v = VIDEOS.find(x => x.video_id === videoId);
  if (!v) return;
  renderVideoList();
  renderMainContent(v);
}}

function renderMainContent(v) {{
  const main = document.getElementById('main-content');
  const [mLabel, mClass] = momentumLabel(v.momentum);

  main.innerHTML = `
    <div class="panel">
      <h2>${{v.title}}</h2>
      <div class="stats-row">
        <div class="stat-box"><div class="label">Total Views</div><div class="value">${{fmt(v.views)}}</div></div>
        <div class="stat-box"><div class="label">Likes</div><div class="value">${{fmt(v.likes)}}</div></div>
        <div class="stat-box"><div class="label">Comments</div><div class="value">${{fmt(v.comments_count)}}</div></div>
        <div class="stat-box"><div class="label">Duration</div><div class="value">${{v.duration_min}}m</div></div>
        <div class="stat-box"><div class="label">7-Day Momentum</div><div class="value" style="color:var(--${{mClass === 'hot' ? 'red' : mClass === 'rising' ? 'green' : 'text2'}})">${{v.momentum > 0 ? '+' : ''}}${{v.momentum}}%</div></div>
      </div>
      <h3>Daily Views (Last ${{VIDEOS[0]?.daily_views?.length || 180}} days)</h3>
      <div class="chart-container"><canvas id="trendChart"></canvas></div>
    </div>

    ${{v.retention && v.retention.length > 0 ? `
    <div class="panel">
      <h2>Audience Retention Curve</h2>
      <div class="chart-container"><canvas id="retentionChart"></canvas></div>
      ${{v.retention_analysis ? `<h3>Retention vs. Script Analysis (Claude)</h3><div class="analysis">${{escapeHtml(v.retention_analysis)}}</div>` : ''}}
    </div>` : ''}}

    ${{v.comment_analysis ? `
    <div class="panel">
      <h2>Comment Themes (Claude Analysis)</h2>
      <div class="analysis">${{escapeHtml(v.comment_analysis)}}</div>
    </div>` : ''}}
  `;

  renderTrendChart(v);
  if (v.retention && v.retention.length > 0) renderRetentionChart(v);
}}

function escapeHtml(text) {{
  return text.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/\\*\\*(.+?)\\*\\*/g, '<strong>$1</strong>')
    .replace(/\\n/g, '<br>');
}}

function renderTrendChart(v) {{
  if (trendChart) trendChart.destroy();
  const ctx = document.getElementById('trendChart');
  if (!ctx || !v.daily_views?.length) return;

  const labels = v.daily_views.map(d => d.date);
  const views = v.daily_views.map(d => d.views);

  // Mark spike days
  const spikeSet = new Set((v.spikes || []).map(s => s.date));
  const pointColors = labels.map(l => spikeSet.has(l) ? '#ff4444' : 'transparent');
  const pointRadii = labels.map(l => spikeSet.has(l) ? 5 : 0);

  trendChart = new Chart(ctx, {{
    type: 'line',
    data: {{
      labels,
      datasets: [{{
        label: 'Daily Views',
        data: views,
        borderColor: '#448aff',
        backgroundColor: 'rgba(68,138,255,0.1)',
        fill: true,
        tension: 0.3,
        pointBackgroundColor: pointColors,
        pointRadius: pointRadii,
        pointHoverRadius: 6,
        borderWidth: 2,
      }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: (ctx) => {{
              const spike = (v.spikes||[]).find(s => s.date === labels[ctx.dataIndex]);
              if (spike) return `${{ctx.parsed.y}} views (${{spike.multiplier}}x avg!)`;
              return `${{ctx.parsed.y}} views`;
            }}
          }}
        }}
      }},
      scales: {{
        x: {{ ticks: {{ maxTicksLimit: 12, color: '#666' }}, grid: {{ color: '#222' }} }},
        y: {{ ticks: {{ color: '#666' }}, grid: {{ color: '#222' }} }}
      }}
    }}
  }});
}}

function renderRetentionChart(v) {{
  if (retentionChart) retentionChart.destroy();
  const ctx = document.getElementById('retentionChart');
  if (!ctx) return;

  const labels = v.retention.map(r => {{
    const totalSec = r.position * v.duration_min * 60;
    const m = Math.floor(totalSec / 60);
    const s = Math.floor(totalSec % 60);
    return `${{m}}:${{String(s).padStart(2,'0')}}`;
  }});

  retentionChart = new Chart(ctx, {{
    type: 'line',
    data: {{
      labels,
      datasets: [
        {{
          label: 'Retention %',
          data: v.retention.map(r => r.retention),
          borderColor: '#00c853',
          backgroundColor: 'rgba(0,200,83,0.1)',
          fill: true, tension: 0.3, borderWidth: 2, pointRadius: 0, pointHoverRadius: 4,
        }},
        {{
          label: 'Relative (vs avg)',
          data: v.retention.map(r => r.relative * 100),
          borderColor: '#aa66cc',
          borderDash: [4, 4],
          tension: 0.3, borderWidth: 1.5, pointRadius: 0, pointHoverRadius: 4, fill: false,
        }}
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{
        tooltip: {{
          callbacks: {{
            title: (items) => `Timestamp: ${{items[0].label}}`,
          }}
        }}
      }},
      scales: {{
        x: {{ ticks: {{ maxTicksLimit: 15, color: '#666' }}, grid: {{ color: '#222' }},
               title: {{ display: true, text: 'Video Timeline', color: '#666' }} }},
        y: {{ ticks: {{ color: '#666', callback: v => v+'%' }}, grid: {{ color: '#222' }},
               title: {{ display: true, text: 'Viewers Remaining', color: '#666' }} }}
      }}
    }}
  }});
}}

// Auto-select the highest-momentum video on load
renderVideoList();
if (VIDEOS.length > 0) {{
  const sorted = [...VIDEOS].sort((a,b) => b.momentum - a.momentum);
  selectVideo(sorted[0].video_id);
}}
</script>
</body>
</html>"""


# ===================================================================
# MAIN
# ===================================================================
def main():
    parser = argparse.ArgumentParser(description="Generate YouTube Analytics Dashboard")
    parser.add_argument("--output", default=None, help="Output HTML file path")
    args = parser.parse_args()

    output_path = args.output or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "youtube_dashboard.html"
    )

    log.info("=" * 60)
    log.info("YouTube Analytics Dashboard Generator")
    log.info("=" * 60)

    if not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY not set — Claude analysis will be skipped")

    creds = get_youtube_credentials()
    youtube = build("youtube", "v3", credentials=creds)
    yt_analytics = build("youtubeAnalytics", "v2", credentials=creds)

    channel_id = get_channel_id(youtube)
    log.info(f"Channel: {channel_id}")

    # Fetch all videos and filter to last TREND_DAYS
    all_videos = fetch_all_video_ids(youtube, channel_id)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=TREND_DAYS)).strftime("%Y-%m-%d")
    videos = [v for v in all_videos if v["published_at"] >= cutoff]
    log.info(f"Found {len(all_videos)} total videos, {len(videos)} in last {TREND_DAYS} days")

    videos_data = []
    for i, v in enumerate(videos, 1):
        vid = v["video_id"]
        log.info(f"\n[{i}/{len(videos)}] {v['title'][:50]}...")

        # Daily views for trend chart
        log.info("  Fetching daily views...")
        daily = fetch_daily_views(yt_analytics, vid, v["published_at"])

        # Spikes & momentum
        spikes = detect_spikes(daily)
        momentum = compute_momentum_score(daily)

        # Retention curve
        log.info("  Fetching retention curve...")
        retention = fetch_retention_curve(yt_analytics, vid, v["published_at"])

        # Transcript
        log.info("  Fetching transcript...")
        transcript = fetch_transcript(vid)

        # Comments
        log.info("  Fetching comments...")
        comments = fetch_comments(youtube, vid, max_comments=80)

        # Claude analysis
        comment_analysis = ""
        retention_analysis = ""

        if comments and ANTHROPIC_API_KEY:
            log.info("  Analyzing comments with Claude...")
            comment_analysis = summarize_comments(v["title"], comments)

        if retention and transcript and ANTHROPIC_API_KEY:
            log.info("  Analyzing retention vs transcript with Claude...")
            retention_analysis = analyze_retention_vs_transcript(
                v["title"], v["duration_min"], retention, transcript
            )

        videos_data.append({
            "video_id": vid,
            "title": v["title"],
            "published_at": v["published_at"],
            "views": v["views"],
            "likes": v["likes"],
            "comments_count": v["comments_count"],
            "duration_min": v["duration_min"],
            "daily_views": daily,
            "spikes": spikes,
            "momentum": momentum,
            "retention": retention,
            "comment_analysis": comment_analysis,
            "retention_analysis": retention_analysis,
        })

    # Generate HTML
    log.info("\nGenerating dashboard HTML...")
    generated_at = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    html = generate_dashboard_html(videos_data, generated_at)

    Path(output_path).write_text(html, encoding="utf-8")
    log.info(f"Dashboard saved to: {output_path}")
    log.info("Done!")


if __name__ == "__main__":
    main()
