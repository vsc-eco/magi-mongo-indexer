#!/usr/bin/env bash
# Alert when the indexer stops polling MongoDB or the node falls behind.
#
# Usage: scripts/check_health.sh <graphql-url> [label]
#   e.g. scripts/check_health.sh https://indexer.example.com/v1/graphql mainnet
#
# Exits 0 when indexer_health.status is "ok", 1 otherwise. If ALERT_WEBHOOK_URL
# is set, a failure is also posted there (Discord and Slack webhooks both work).
# Meant for cron, e.g. every 5 minutes:
#   */5 * * * * ALERT_WEBHOOK_URL=https://... /path/to/scripts/check_health.sh <url> mainnet
set -u

url="${1:?usage: check_health.sh <graphql-url> [label]}"
label="${2:-indexer}"

resp=$(curl -s -m 20 "$url" -H 'content-type: application/json' \
  -d '{"query":"{ indexer_health { status seconds_since_poll mongo_head_block mongo_head_time last_error last_error_at } }"}')

if printf '%s' "$resp" | grep -Eq '"status" *: *"ok"'; then
  exit 0
fi

msg="[$label] indexer health check failed: ${resp:-no response from $url}"
echo "$msg" >&2

if [ -n "${ALERT_WEBHOOK_URL:-}" ]; then
  payload=$(printf '%s' "$msg" | head -c 1800 | sed 's/\\/\\\\/g; s/"/\\"/g' | tr -d '\n\r')
  curl -s -m 20 -H 'content-type: application/json' \
    -d "{\"content\":\"$payload\",\"text\":\"$payload\"}" "$ALERT_WEBHOOK_URL" >/dev/null
fi
exit 1
