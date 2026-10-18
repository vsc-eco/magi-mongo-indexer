#!/usr/bin/env python3
"""Alert when a magi-mongo-indexer stops indexing.

Usage: scripts/check_health.py <graphql-url>
  e.g. scripts/check_health.py https://indexer.example.com/v1/graphql

Run it from cron every few minutes. It reads indexer_health.status and only
notifies on changes, not on every run:
  - a problem is reported after FAIL_THRESHOLD consecutive failed checks
    (default 2), so a single blip or a restart stays quiet
  - while it lasts, a reminder goes out every REMIND_MINUTES (default 60)
  - one "recovered" message is sent when it is healthy again

"starting" (restart, first poll not done yet) never counts as a failure.

Environment:
  INDEXER_NAME       name shown in alerts (default: the URL's host, e.g.
                     indexer.example.com)
  ALERT_WEBHOOK_URL  Discord webhook (sent as an embed) or Slack webhook
                     (plain text). Unset: messages go to stderr only.
  FAIL_THRESHOLD     consecutive failed checks before alerting (default 2)
  REMIND_MINUTES     minutes between reminders while down (default 60)
  STATE_DIR          where the per-indexer state file lives
                     (default ~/.local/state/magi-indexer-health)

Exit code: 0 healthy, 1 failing (this check), 2 usage error.
Standard library only.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlparse

QUERY = ("{ indexer_health { status seconds_since_poll mongo_head_block "
         "mongo_head_time last_error last_error_at latest_block_height } }")

RED, ORANGE, GREEN = 0xE74C3C, 0xE67E22, 0x2ECC71


def fetch_health(url):
    """Return (kind, health) where kind is ok/starting/stale/node_behind/unreachable."""
    req = urllib.request.Request(
        url, data=json.dumps({"query": QUERY}).encode(),
        headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = json.load(resp)
    except (urllib.error.URLError, OSError, ValueError) as e:
        return "unreachable", {"error": str(e)}
    rows = (body.get("data") or {}).get("indexer_health") or []
    if not rows:
        return "unreachable", {"error": json.dumps(body.get("errors") or body)[:500]}
    health = rows[0]
    status = health.get("status")
    if status not in ("ok", "starting", "stale", "node_behind"):
        return "unreachable", {"error": f"unexpected status {status!r}"}
    return status, health


def human(seconds):
    if seconds is None:
        return "unknown"
    days, rest = divmod(int(seconds), 86400)
    hours, rest = divmod(rest, 3600)
    minutes, secs = divmod(rest, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def age_of(ts):
    """Seconds since an ISO timestamp from Hasura, or None."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds()


TITLES = {
    "stale": ("🔴 Indexer stalled", RED,
              "No successful poll of MongoDB for over 10 minutes. Data is not updating."),
    "node_behind": ("🟠 Node behind", ORANGE,
                    "The indexer polls fine, but the node stopped syncing: MongoDB's newest Hive block is over 10 minutes old."),
    "unreachable": ("🔴 Indexer unreachable", RED,
                    "The health endpoint did not answer with a status."),
}


def build_message(event, kind, health, name, url, down_since):
    """event: alert | reminder | recovered."""
    if event == "recovered":
        title, color = "🟢 Indexer recovered", GREEN
        desc = "Health is back to ok."
        if down_since:
            desc += f" Down for about {human(time.time() - down_since)}."
    else:
        title, color, desc = TITLES[kind]
        if event == "reminder" and down_since:
            title = f"{title[:2]}Still down: {title[2:].lower()}"
            desc += f" Down for {human(time.time() - down_since)}."

    fields = [{"name": "Indexer", "value": name, "inline": True},
              {"name": "Status", "value": kind if event != "recovered" else "ok", "inline": True}]
    if "status" in health:
        fields.append({"name": "Last successful poll",
                       "value": f"{human(health.get('seconds_since_poll'))} ago", "inline": True})
        head = health.get("mongo_head_block")
        head_age = age_of(health.get("mongo_head_time"))
        fields.append({"name": "Mongo head block",
                       "value": f"{head} ({human(head_age)} old)" if head else "unknown", "inline": True})
        fields.append({"name": "Indexed up to block",
                       "value": str(health.get("latest_block_height")), "inline": True})
        if health.get("last_error") and event != "recovered":
            fields.append({"name": "Last error",
                           "value": f"```{health['last_error'][:950]}```", "inline": False})
    elif health.get("error"):
        fields.append({"name": "Error", "value": f"```{health['error'][:950]}```", "inline": False})

    title = f"{title} · {name}"
    embed = {"title": title, "description": desc, "color": color, "url": url,
             "fields": fields, "footer": {"text": "magi-mongo-indexer health"},
             "timestamp": datetime.now(timezone.utc).isoformat()}
    text = f"{title}: {desc}" + "".join(
        f"\n{f['name']}: {f['value'].strip('`')}" for f in fields[1:])
    return embed, text


def notify(webhook, embed, text):
    print(text, file=sys.stderr)
    if not webhook:
        return
    host = urlparse(webhook).hostname or ""
    if host.endswith("discord.com") or host.endswith("discordapp.com"):
        payload = {"embeds": [embed]}
    else:
        payload = {"text": text}
    req = urllib.request.Request(
        webhook, data=json.dumps(payload).encode(),
        headers={"content-type": "application/json", "user-agent": "magi-indexer-health"})
    try:
        urllib.request.urlopen(req, timeout=20).close()
    except (urllib.error.URLError, OSError) as e:
        print(f"webhook post failed: {e}", file=sys.stderr)


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    url = sys.argv[1]
    name = os.environ.get("INDEXER_NAME") or urlparse(url).hostname or url
    webhook = os.environ.get("ALERT_WEBHOOK_URL", "")
    threshold = int(os.environ.get("FAIL_THRESHOLD", "2"))
    remind = int(os.environ.get("REMIND_MINUTES", "60")) * 60
    state_dir = os.path.expanduser(os.environ.get(
        "STATE_DIR", "~/.local/state/magi-indexer-health"))
    os.makedirs(state_dir, exist_ok=True)
    state_file = os.path.join(state_dir, "".join(c if c.isalnum() or c in ".-_" else "_" for c in name) + ".json")

    try:
        with open(state_file) as f:
            state = json.load(f)
    except (OSError, ValueError):
        state = {}
    alerting = state.get("alerting", False)
    fails = state.get("fails", 0)

    kind, health = fetch_health(url)
    now = time.time()

    if kind in ("ok", "starting"):
        if alerting and kind == "ok":
            embed, text = build_message("recovered", kind, health, name, url, state.get("down_since"))
            notify(webhook, embed, text)
            state = {}
        elif not alerting:
            state = {}
        # "starting" while alerting: keep the alert open until a real ok
    else:
        fails += 1
        state["fails"] = fails
        state.setdefault("down_since", now)
        if not alerting and fails >= threshold:
            embed, text = build_message("alert", kind, health, name, url, state["down_since"])
            notify(webhook, embed, text)
            state.update(alerting=True, last_alert=now, kind=kind)
        elif alerting and (now - state.get("last_alert", 0) >= remind or kind != state.get("kind")):
            embed, text = build_message("reminder", kind, health, name, url, state["down_since"])
            notify(webhook, embed, text)
            state.update(last_alert=now, kind=kind)

    tmp = state_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, state_file)
    return 0 if kind in ("ok", "starting") else 1


if __name__ == "__main__":
    sys.exit(main())
