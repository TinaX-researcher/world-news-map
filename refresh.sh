#!/usr/bin/env bash
set -euo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

cd "$(dirname "$0")"

echo "=== $(date) ==="
python3 update_data.py

git add data.js
if git diff --cached --quiet; then
    echo "No data changes; nothing to commit."
    exit 0
fi

git commit -m "weekly data refresh: $(date +%Y-%m-%d)"
git push
echo "Pushed."
