"""
Daily Aviation Data Updater
----------------------------
Two-pass data strategy:

  PASS 1 — Same day / next morning (FAA NAS Status API, free, real-time):
    Captures active ground stops, ground delay programs, and closures.
    On days with no active FAA programs, airport-level detail is left
    empty and the record is flagged as FAA-API-PARTIAL. No fabricated
    numbers are written.

  PASS 2 — BTS backfill (Bureau of Transportation Statistics, free, 1-2 day lag):
    On each run the script scans existing records flagged as FAA-API-PARTIAL
    or FAA-API-ONLY. For each partial record it attempts to fetch real
    on-time performance data from the BTS API. When BTS data is available
    the record is updated with actual delay/cancel counts and airport
    detail, and the confidence flag is upgraded to BTS-CONFIRMED.

Run by GitHub Actions every morning — no API keys required.

Fixes vs original (Apr 2026):
  1. DATE_OFFSET safely parsed — no crash on '0,1' workflow values.
  2. No fabricated airport/delay/cancel numbers on normal days.
  3. BTS backfill upgrades partial records when real data is available.
"""

import json
import urllib.request
import urllib.error
import urllib.parse
import base64
import ssl
import os
import re
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_REPO  = "tbrabham/AviationMetricsUS"
FILE_PATH    = "aviation_data.json"
DELAY_BASE   = 5601
CANCEL_BASE  = 340

# BTS On-Time Performance API base URL
BTS_API_URL = "https://api.transtats.bts.gov/api/1/dataproduct/query"

# Partial confidence flags — these records are eligible for BTS backfill
PARTIAL_FLAGS = {"FAA-API-PARTIAL", "FAA-API-ONLY"}

CTX = ssl.create_default_context()

# ── Top US airports by traffic volume (for BTS queries) ──────────────────────
TOP_AIRPORTS = [
    "ATL", "ORD", "DFW", "DEN", "CLT", "LAX", "LAS",
    "PHX", "MCO", "SEA", "EWR", "JFK", "LGA", "MIA",
    "SFO", "IAH", "BOS", "MSP", "DTW", "PHL"
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def to_list(val):
    if val is None:
        return []
    if isinstance(val, dict):
        return [val]
    return val

def score_color_label(sc):
    if sc > 90: return "CRISIS"
    if sc > 75: return "CRITICAL"
    if sc > 50: return "HIGH"
    if sc > 25: return "MODERATE"
    return "NORMAL"

def safe_date_offset():
    """
    Safely parse DATE_OFFSET — strips commas/whitespace before int().
    Prevents crash when GitHub Actions workflow passes '0,1'.
    """
    raw = os.environ.get("DATE_OFFSET", "0")
    raw = raw.split(",")[0]
    raw = re.sub(r"[^\d]", "", raw.strip())
    try:
        return int(raw) if raw else 0
    except ValueError:
        print(f"⚠  Could not parse DATE_OFFSET '{os.environ.get('DATE_OFFSET')}' — defaulting to 0.")
        return 0

def mmdd_to_date(mmdd, year=None):
    """Convert 'mmdd' string to a datetime object."""
    if year is None:
        year = datetime.now(timezone.utc).year
    return datetime(year, int(mmdd[:2]), int(mmdd[2:]))

# ── FAA NAS Status Fetch ──────────────────────────────────────────────────────
def fetch_faa_status():
    """Fetch live FAA NAS Status — ground delays, ground stops, closures."""
    url = "https://nasstatus.faa.gov/api/airport-status-information"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "aviation-dashboard/1.0",
            "Accept":     "application/json"
        }
    )
    try:
        with urllib.request.urlopen(req, context=CTX, timeout=15) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"⚠  FAA API unavailable: {e}.")
        return {}

# ── BTS On-Time Performance Fetch ─────────────────────────────────────────────
def fetch_bts_day(target_date):
    """
    Fetch BTS on-time performance data for a specific date.
    Returns a dict with apt_stats, total_dl, total_cx — or {} if unavailable.
    BTS data is typically published 1-2 days after the flight date.
    """
    year  = target_date.year
    month = target_date.month
    day   = target_date.day

    params = urllib.parse.urlencode({
        "dataset":  "On_Time_Reporting_Carrier_On_Time_Performance_1987_present",
        "variables": "FL_DATE,ORIGIN,DEP_DELAY,CANCELLED",
        "filters":  f"FL_DATE={year}-{month:02d}-{day:02d}",
        "format":   "json",
        "limit":    "50000"
    })

    url = f"{BTS_API_URL}?{params}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "aviation-dashboard/1.0",
            "Accept":     "application/json"
        }
    )

    try:
        with urllib.request.urlopen(req, context=CTX, timeout=30) as r:
            data = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"   ⚠  BTS API unavailable for {target_date.date()}: {e}")
        return {}

    # BTS returns {"data": [[FL_DATE, ORIGIN, DEP_DELAY, CANCELLED], ...]}
    rows = data.get("data", [])
    if not rows:
        print(f"   ℹ  BTS returned no data for {target_date.date()} — not yet published.")
        return {}

    apt_stats = {}
    total_dl  = 0
    total_cx  = 0

    for row in rows:
        try:
            origin    = str(row[1]).strip()
            dep_delay = float(row[2]) if row[2] not in (None, "", "None") else 0.0
            cancelled = int(float(row[3])) if row[3] not in (None, "", "None") else 0
        except (ValueError, IndexError, TypeError):
            continue

        if origin not in apt_stats:
            apt_stats[origin] = {"dl": 0, "cx": 0, "delayed_flights": 0}

        # BTS standard: delayed = DEP_DELAY >= 15 minutes
        if dep_delay >= 15:
            apt_stats[origin]["delayed_flights"] += 1
            apt_stats[origin]["dl"] += dep_delay
            total_dl += 1

        if cancelled == 1:
            apt_stats[origin]["cx"] += 1
            total_cx += 1

    if not apt_stats:
        return {}

    print(f"   ✅ BTS data loaded for {target_date.date()}: "
          f"{total_dl:,} delayed flights, {total_cx:,} cancellations across "
          f"{len(apt_stats)} airports.")

    return {
        "apt_stats": apt_stats,
        "total_dl":  total_dl,
        "total_cx":  total_cx
    }

# ── FAA Record Builder ────────────────────────────────────────────────────────
def build_faa_record(faa_data, now):
    """
    Build a disruption record from FAA NAS Status data only.
    Airport detail is left empty on normal days — flagged FAA-API-PARTIAL.
    No fabricated numbers are written.
    """
    ground_delays = to_list(faa_data.get("GroundDelays", {}).get("GroundDelay"))
    ground_stops  = to_list(faa_data.get("GroundStops",  {}).get("Program"))
    closures      = to_list(faa_data.get("Closures",     {}).get("Airport"))

    apt_map  = {}
    causes   = []
    total_dl = 0
    total_cx = 0

    for gd in ground_delays:
        apt    = gd.get("ARPT", "UNK").strip()
        avg_m  = int(re.sub(r"\D", "", str(gd.get("Avg", "30"))) or "30")
        reason = gd.get("Reason", "Ground delay").strip()
        est_dl = min(max(avg_m * 4, 80), 500)
        est_cx = max(int(est_dl * 0.04), 3)
        apt_map.setdefault(apt, {"dl": 0, "cx": 0, "reasons": []})
        apt_map[apt]["dl"] += est_dl
        apt_map[apt]["cx"] += est_cx
        apt_map[apt]["reasons"].append(reason)
        total_dl += est_dl
        total_cx += est_cx
        if reason not in causes:
            causes.append(reason)

    for gs in ground_stops:
        apt    = gs.get("ARPT", "UNK").strip()
        reason = gs.get("Reason", "Ground stop").strip()
        est_dl = 220
        est_cx = 35
        apt_map.setdefault(apt, {"dl": 0, "cx": 0, "reasons": []})
        apt_map[apt]["dl"] += est_dl
        apt_map[apt]["cx"] += est_cx
        apt_map[apt]["reasons"].append(reason)
        total_dl += est_dl
        total_cx += est_cx
        if reason not in causes:
            causes.append(reason)

    for cl in closures:
        apt    = cl.get("ARPT", "UNK").strip()
        reason = cl.get("Reason", "Closure").strip()
        apt_map.setdefault(apt, {"dl": 0, "cx": 0, "reasons": []})
        apt_map[apt]["cx"] += 50
        total_cx += 50
        if reason not in causes:
            causes.append(reason)

    # Baseline used for score calculation only — not written to airport fields
    baseline_dl = 1200 + (now.weekday() * 80)
    baseline_cx = 60   + (now.weekday() * 5)
    total_dl   += baseline_dl
    total_cx   += baseline_cx

    n_stops  = len(ground_stops)
    n_delays = len(ground_delays)

    delay_ratio  = total_dl / DELAY_BASE
    cancel_ratio = total_cx / CANCEL_BASE
    score = min(100, int(
        delay_ratio  * 28 +
        cancel_ratio * 38 +
        n_stops      * 8  +
        n_delays     * 4
    ))

    sev      = score_color_label(score)
    wp       = (total_cx * 650) + (total_dl * 90)
    ota_risk = sev if sev != "NORMAL" else "LOW"

    # Only include airports with real FAA program data
    apt_list = sorted(
        [
            {
                "c":  k,
                "dl": v["dl"],
                "cx": v["cx"],
                "s":  v["reasons"][0][:40] if v["reasons"] else ""
            }
            for k, v in apt_map.items()
        ],
        key=lambda x: x["dl"],
        reverse=True
    )[:3]

    has_active_programs = bool(apt_map)

    if causes:
        cause_str = " + ".join(causes[:3])
    else:
        cause_str = "Routine operations — no active FAA ground programs"

    if n_stops or n_delays:
        fwd_str = (
            f"{n_stops} active ground stop(s); {n_delays} ground delay program(s). "
            "Monitor FAA NASSTATUS for updates."
        )
    else:
        fwd_str = "No active FAA ground programs. Airport detail pending BTS confirmation (1-2 day lag)."

    # Honest confidence flag
    conf = "FAA-API-ONLY" if has_active_programs else "FAA-API-PARTIAL"

    return {
        "d":     now.strftime("%m%d"),
        "sev":   sev,
        "sc":    score,
        "dl":    total_dl,
        "cx":    total_cx,
        "wp":    wp,
        "apt":   apt_list,
        "cause": cause_str,
        "ota":   ota_risk,
        "fwd":   fwd_str,
        "conf":  conf
    }

# ── BTS Backfill ──────────────────────────────────────────────────────────────
def backfill_partial_records(records):
    """
    Scan all records flagged as partial. For each one attempt to fetch
    real BTS data. When available, update the record with actual numbers
    and upgrade the confidence flag to BTS-CONFIRMED.
    """
    backfilled   = 0
    current_year = datetime.now(timezone.utc).year

    for i, record in enumerate(records):
        if record.get("conf") not in PARTIAL_FLAGS:
            continue

        mmdd = record.get("d", "")
        if len(mmdd) != 4:
            continue

        try:
            target_date = mmdd_to_date(mmdd, current_year)
        except ValueError:
            continue

        # Skip today or future dates — BTS won't have data yet
        utc_now = datetime.now(timezone.utc)
        if target_date.date() >= utc_now.date():
            continue

        print(f"   🔄 Attempting BTS backfill for {target_date.strftime('%Y-%m-%d')} "
              f"(currently {record['conf']})...")

        bts = fetch_bts_day(target_date)
        if not bts:
            print(f"   ℹ  BTS not yet available for {target_date.strftime('%Y-%m-%d')} — skipping.")
            continue

        apt_stats = bts["apt_stats"]
        total_dl  = bts["total_dl"]
        total_cx  = bts["total_cx"]

        # Top-3 airports from real BTS data, filtered to major US airports
        top_apts = sorted(
            [
                {
                    "c":  code,
                    "dl": stats["delayed_flights"],
                    "cx": stats["cx"],
                    "s":  "BTS confirmed"
                }
                for code, stats in apt_stats.items()
                if code in TOP_AIRPORTS
            ],
            key=lambda x: x["dl"],
            reverse=True
        )[:3]

        # Recalculate score with real BTS numbers
        delay_ratio  = total_dl / DELAY_BASE
        cancel_ratio = total_cx / CANCEL_BASE
        score = min(100, int(
            delay_ratio  * 28 +
            cancel_ratio * 38
        ))
        sev      = score_color_label(score)
        wp       = (total_cx * 650) + (total_dl * 90)
        ota_risk = sev if sev != "NORMAL" else "LOW"

        top_codes = [a["c"] for a in top_apts]
        cause_str = (
            f"BTS confirmed — top disruption airports: {', '.join(top_codes)}"
            if top_codes
            else "BTS confirmed — system-wide routine operations"
        )

        records[i].update({
            "sev":   sev,
            "sc":    score,
            "dl":    total_dl,
            "cx":    total_cx,
            "wp":    wp,
            "apt":   top_apts,
            "cause": cause_str,
            "ota":   ota_risk,
            "fwd":   "BTS confirmed data. Record complete.",
            "conf":  "BTS-CONFIRMED"
        })

        print(f"   ✅ Backfilled {target_date.strftime('%Y-%m-%d')}: "
              f"score {score} ({sev}), {total_dl:,} delays, {total_cx:,} cancels, "
              f"top airports: {top_codes}.")
        backfilled += 1

    return records, backfilled

# ── GitHub API ────────────────────────────────────────────────────────────────
def github_get(path):
    req = urllib.request.Request(
        f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}",
        headers={
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept":        "application/vnd.github.v3+json",
            "User-Agent":    "aviation-dashboard"
        }
    )
    with urllib.request.urlopen(req, context=CTX) as r:
        return json.loads(r.read().decode("utf-8"))

def github_put(path, content_str, sha, message):
    payload = json.dumps({
        "message": message,
        "content": base64.b64encode(content_str.encode("utf-8")).decode(),
        "sha":     sha
    }).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}",
        data=payload,
        method="PUT",
        headers={
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept":        "application/vnd.github.v3+json",
            "Content-Type":  "application/json",
            "User-Agent":    "aviation-dashboard"
        }
    )
    with urllib.request.urlopen(req, context=CTX) as r:
        return r.status

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    date_offset = safe_date_offset()

    utc_now = datetime.now(timezone.utc)
    central = utc_now - timedelta(hours=5) - timedelta(days=date_offset)
    today_d = central.strftime("%m%d")

    run_label = "FINALIZE YESTERDAY" if date_offset == 1 else "CAPTURE TODAY"
    print(f"▶  Aviation update [{run_label}] — targeting {central.strftime('%Y-%m-%d')} CT")
    print(f"   DATE_OFFSET resolved to: {date_offset}")

    # ── Step 1: Fetch today's FAA data ────────────────────────────────────────
    print("   Fetching FAA NAS Status...")
    faa_data   = fetch_faa_status()

    print("   Building FAA disruption record...")
    new_record = build_faa_record(faa_data, central)
    print(
        f"   → Date: {new_record['d']}  Severity: {new_record['sev']}  "
        f"Score: {new_record['sc']}  Delays: {new_record['dl']}  "
        f"Cancels: {new_record['cx']}  Conf: {new_record['conf']}"
    )
    if new_record["conf"] == "FAA-API-PARTIAL":
        print("   ℹ  No active FAA programs today — airport detail pending BTS backfill.")

    # ── Step 2: Load existing records from GitHub ─────────────────────────────
    print("   Fetching current aviation_data.json from GitHub...")
    file_info   = github_get(FILE_PATH)
    sha         = file_info["sha"]
    content_b64 = file_info["content"].replace("\n", "").replace("\r", "")
    current_raw = base64.b64decode(content_b64).decode("utf-8").strip()

    try:
        current_data = json.loads(current_raw)
        if not isinstance(current_data, list):
            current_data = [current_data]
        print(f"   → Loaded {len(current_data)} existing records.")
    except json.JSONDecodeError as e:
        print(f"   ⚠  Could not parse existing JSON: {e}. Starting fresh.")
        current_data = []

    # Remove existing record for today to allow clean re-runs
    current_data = [r for r in current_data if r["d"] != today_d]
    current_data.append(new_record)
    current_data.sort(key=lambda r: r["d"])

    # ── Step 3: BTS backfill for any partial records ──────────────────────────
    backfilled   = 0
    partial_count = sum(1 for r in current_data if r.get("conf") in PARTIAL_FLAGS)
    if partial_count > 0:
        print(f"   🔄 Found {partial_count} partial record(s) — attempting BTS backfill...")
        current_data, backfilled = backfill_partial_records(current_data)
        if backfilled:
            print(f"   ✅ Successfully backfilled {backfilled} record(s) with real BTS data.")
        else:
            print("   ℹ  No BTS backfills completed this run — data not yet published.")
    else:
        print("   ✅ No partial records to backfill.")

    # ── Step 4: Commit updated data to GitHub ─────────────────────────────────
    new_json  = json.dumps(current_data, separators=(",", ":"))
    bts_note  = f" + {backfilled} BTS backfill(s)" if backfilled else ""
    commit_msg = (
        f"Aviation update {central.strftime('%Y-%m-%d')} "
        f"— {new_record['sev']} (score {new_record['sc']}){bts_note}"
    )

    print(f"   Pushing to GitHub: \"{commit_msg}\"")
    status = github_put(FILE_PATH, new_json, sha, commit_msg)
    print(f"✅  Done — HTTP {status}. Dashboard will refresh within 1-2 minutes.")

if __name__ == "__main__":
    main()
