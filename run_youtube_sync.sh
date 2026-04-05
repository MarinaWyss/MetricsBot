#!/bin/bash
# YouTube Metrics Sync - Local runner
# This script is called by macOS launchd daily to fetch fresh YouTube metrics.
# It activates the Python venv, runs the sync script (and optionally the dashboard),
# and logs output.

SCRIPT_DIR="$HOME/Desktop/cowork/MetricsBot"
LOG_FILE="$SCRIPT_DIR/sync.log"
VENV="$SCRIPT_DIR/venv/bin/activate"

echo "========================================" >> "$LOG_FILE"
echo "YouTube Metrics Sync — $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Activate venv
if [ -f "$VENV" ]; then
    source "$VENV"
else
    echo "ERROR: venv not found at $VENV" >> "$LOG_FILE"
    exit 1
fi

cd "$SCRIPT_DIR"

# Run the metrics sync (fetches from YouTube API → writes youtube_metrics.json)
echo "Running youtube_metrics_sync.py..." >> "$LOG_FILE"
python3 youtube_metrics_sync.py >> "$LOG_FILE" 2>&1
SYNC_EXIT=$?

if [ $SYNC_EXIT -ne 0 ]; then
    echo "ERROR: youtube_metrics_sync.py exited with code $SYNC_EXIT" >> "$LOG_FILE"
else
    echo "Sync completed successfully." >> "$LOG_FILE"
fi

# Run the dashboard generator (fetches trends, comments, retention → writes HTML)
echo "Running youtube_dashboard_generator.py..." >> "$LOG_FILE"
python3 youtube_dashboard_generator.py >> "$LOG_FILE" 2>&1
DASH_EXIT=$?

if [ $DASH_EXIT -ne 0 ]; then
    echo "ERROR: youtube_dashboard_generator.py exited with code $DASH_EXIT" >> "$LOG_FILE"
else
    echo "Dashboard generated successfully." >> "$LOG_FILE"
fi

# Push updated dashboard + metrics to GitHub (for GitHub Pages)
if [ -f "$SCRIPT_DIR/youtube_dashboard.html" ]; then
    echo "Pushing to GitHub..." >> "$LOG_FILE"
    cd "$SCRIPT_DIR"
    git add youtube_dashboard.html youtube_metrics.json >> "$LOG_FILE" 2>&1
    git commit -m "Daily metrics update $(date +%Y-%m-%d)" >> "$LOG_FILE" 2>&1
    git push >> "$LOG_FILE" 2>&1
    if [ $? -eq 0 ]; then
        echo "GitHub push successful." >> "$LOG_FILE"
    else
        echo "WARNING: GitHub push failed." >> "$LOG_FILE"
    fi
fi

echo "Done at $(date)" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
