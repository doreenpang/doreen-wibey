#!/usr/bin/env python3
"""
refresh_hotdog.py
Queries BigQuery for the latest Hot Dog Label Experiment data and
embeds it as a static snapshot into hot-dog-label-dashboard.html.

Run locally:
    python3 scripts/refresh_hotdog.py

Run in CI (GCP_CREDENTIALS env var must be set):
    GCP_CREDENTIALS='<json>' python3 scripts/refresh_hotdog.py
"""

import json
import os
import re
import sys
from datetime import date, timedelta, timezone, datetime

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT          = "wmt-driver-insights"
MARKET           = "Walmart Secaucus"
PILOT_START_DATE = "2026-03-09"
PRE_PILOT_START  = "2026-02-09"

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT    = os.path.dirname(SCRIPT_DIR)
HTML_PATH    = os.path.join(REPO_ROOT, "hot-dog-label-dashboard.html")
CREDS_FILE   = os.path.join(REPO_ROOT, ".gcp_credentials.json")


def write_credentials():
    """Write GCP_CREDENTIALS env var to a temp file for ADC."""
    creds_json = os.environ.get("GCP_CREDENTIALS", "").strip()
    print(f"  GCP_CREDENTIALS length : {len(creds_json)} chars")
    if not creds_json:
        print("ℹ  No GCP_CREDENTIALS env var — using local ADC")
        return

    # Validate it's parseable JSON before writing
    try:
        parsed = json.loads(creds_json)
        print(f"  Credential type        : {parsed.get('type', 'unknown')}")
    except json.JSONDecodeError as e:
        print(f"  ✗ GCP_CREDENTIALS is not valid JSON: {e}", file=sys.stderr)
        print(f"  First 100 chars: {repr(creds_json[:100])}", file=sys.stderr)
        sys.exit(1)

    with open(CREDS_FILE, "w") as f:
        f.write(creds_json)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDS_FILE
    print(f"✓ Credentials written to {CREDS_FILE}")


def query_bigquery():
    import traceback
    from google.cloud import bigquery
    from google.auth import default as google_auth_default

    # Print credential info for debugging (no secrets)
    try:
        creds, project = google_auth_default()
        print(f"  Auth type    : {type(creds).__name__}")
        print(f"  Auth project : {project}")
        print(f"  Token valid  : {creds.valid}")
        print(f"  Expiry       : {getattr(creds, 'expiry', 'N/A')}")
    except Exception as auth_err:
        print(f"  ⚠ Auth check failed: {auth_err}", file=sys.stderr)
        traceback.print_exc()

    try:
        client = bigquery.Client(project=PROJECT)
    except Exception as e:
        print(f"  ✗ Client init failed: {e}", file=sys.stderr)
        traceback.print_exc()
        raise

    sql = f"""
    SELECT
      SUBSTR(CreateDate_UTC, 1, 10) AS day,
      ai_issue,
      COUNT(*)                      AS contact_count
    FROM `{PROJECT}.PIXIE.airr_tagging_embedding`
    WHERE MARKET_NM = '{MARKET}'
      AND ai_issue IN (
        'Drop-Off Label Missing',
        'Drop-Off Label Scan Malfunction',
        'Drop-Off Label Inaccurate'
      )
      AND CreateDate_UTC >= '{PRE_PILOT_START}'
    GROUP BY day, ai_issue
    ORDER BY day ASC
    """

    print(f"  Running BigQuery query on project={PROJECT}…")
    try:
        rows = list(client.query(sql).result())
    except Exception as e:
        print(f"  ✗ Query failed: {e}", file=sys.stderr)
        traceback.print_exc()
        raise
    print(f"  Got {len(rows)} raw rows from BigQuery")
    return rows


def build_days(rows):
    """Group by date and fill calendar gaps with zeros."""
    by_date = {}
    for row in rows:
        d    = str(row["day"])
        cnt  = int(row["contact_count"])
        if d not in by_date:
            by_date[d] = {"missing": 0, "scan": 0, "inaccurate": 0}
        if row["ai_issue"] == "Drop-Off Label Missing":
            by_date[d]["missing"]    += cnt
        elif row["ai_issue"] == "Drop-Off Label Scan Malfunction":
            by_date[d]["scan"]       += cnt
        elif row["ai_issue"] == "Drop-Off Label Inaccurate":
            by_date[d]["inaccurate"] += cnt

    result = []
    cur  = date.fromisoformat(PRE_PILOT_START)
    today = date.today()
    while cur <= today:
        key    = cur.isoformat()
        label  = cur.strftime("%-m/%-d")          # "3/9", "2/15" etc.
        counts = by_date.get(key, {"missing": 0, "scan": 0, "inaccurate": 0})
        result.append({
            "date":       label,
            "missing":    counts["missing"],
            "scan":       counts["scan"],
            "inaccurate": counts["inaccurate"],
            "isPilot":    key >= PILOT_START_DATE,
        })
        cur += timedelta(days=1)

    return result


def embed_into_html(days):
    """Replace the static data block in the HTML file."""
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    now_utc = datetime.now(timezone.utc).isoformat()
    api_data = {
        "days":          days,
        "lastRefreshed": now_utc,
        "nextRefresh":   "7:00 AM CST daily (GitHub Actions)",
    }

    new_block = (
        "  // ─── Static data (embedded at build time) ────────────────────────────────\n"
        f"  const STATIC_DATA = {json.dumps(api_data, separators=(',', ':'))};\n"
        "\n"
        "  async function init() {\n"
        "    const { days, lastRefreshed, nextRefresh } = STATIC_DATA;\n"
        "    renderDashboard(days);\n"
        "    const ts = lastRefreshed\n"
        "      ? new Date(lastRefreshed).toLocaleString('en-US', { month:'short', day:'numeric', year:'numeric', hour:'2-digit', minute:'2-digit' })\n"
        "      : '—';\n"
        "    document.getElementById('lastRefreshedTime').textContent = ts;\n"
        "    document.getElementById('refresh-status').innerHTML =\n"
        "      `✓ Snapshot · As of ${ts} · <em style=\"opacity:.6\">Live version requires local server</em>`;\n"
        "  }"
    )

    # Replace existing static block (between the marker comment and closing brace of init)
    pattern = re.compile(
        r'  // ─── Static data \(embedded at build time\).*?  \}',
        re.DOTALL
    )
    if pattern.search(html):
        html = pattern.sub(new_block, html)
        print(f"  ✓ Replaced existing static data block ({len(days)} days)")
    else:
        # Fallback: replace old server-fetch init block
        old_pattern = re.compile(
            r'  // ─── Init: fetch live data from server.*?  init\(\);',
            re.DOTALL
        )
        if old_pattern.search(html):
            html = old_pattern.sub(new_block + "\n\n  init();", html)
            print(f"  ✓ Replaced server-fetch init block ({len(days)} days)")
        else:
            print("  ✗ Could not find init block to replace!", file=sys.stderr)
            sys.exit(1)

    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  ✓ Saved {HTML_PATH}")


def cleanup():
    if os.path.exists(CREDS_FILE):
        os.remove(CREDS_FILE)


def main():
    print("🌭  Hot Dog Dashboard Refresh")
    print(f"    Target: {HTML_PATH}")

    write_credentials()

    try:
        rows = query_bigquery()
        days = build_days(rows)
        print(f"  Built {len(days)} day entries (from {PRE_PILOT_START} to today)")
        embed_into_html(days)
        print("✅  Done")
    finally:
        cleanup()


if __name__ == "__main__":
    main()
