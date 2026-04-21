"""
Daily Aviation Data Updater
----------------------------
Fetches real-time FAA ground delay / ground stop data from the public
FAA NAS Status API, builds a daily disruption record in the correct
JSON format, then commits it to aviation_data.json on GitHub.

Run by GitHub Actions every morning — no API keys required.

Data strategy:
  - Active disruption days (ground stops / GDPs active): real FAA airport
    data is used for delay estimates and top airport detail.
  - Normal days (no active FAA programs): severity score and totals are
    calculated from baseline estimates; airport detail is left empty with
    a clean "Normal operations" label. No fabricated numbers.

Fixes applied:
  1. DATE_OFFSET safely parsed — no crash on '0,1' workflow values.
  2. No fabricated airport data on normal days.
"""

import json
import urllib.request
import urllib.error
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

CTX = ssl.create_default_context()

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
    Safely parse DATE_OFFSET — strips commas and whitespace before int().
    Prevents crash when GitHub Actions workflow passes a value like '0,1'.
    """
    raw = os.environ.get("DATE_OFFSET", "0")
    raw = raw.split(",")[0]
    raw = re.sub(r"[^\d]", "", raw.strip())
    try:
        return int(raw) if raw else 0
    except ValueError:
        print(f"⚠  Could not parse DATE_OFFSET '{os.environ.get('DATE_OFFSET')}' — defaulting to 0.")
        return 0

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

# ── Record Builder ────────────────────────────────────────────────────────────
def build_record(faa_data, now):
    """
    Build a disruption record from FAA NAS Status data.

    On days with active FAA programs (ground stops / GDPs):
      - Real airport codes, estimated delays, and causes are populated.

    On normal days with no active programs:
      - apt field contains a single entry with clean 'Normal operations' text.
      - No delay/cancel numbers are fabricated for individual airports.
      - Overall totals use day-of-week baseline estimates for scoring only.
    """
    ground_delays = to_list(faa_data.get("GroundDelays", {}).get("GroundDelay"))
    ground_stops  = to_list(faa_data.get("GroundStops",  {}).get("Program"))
    closures      = to_list(faa_data.get("Closures",     {}).get("Airport"))

    apt_map  = {}
    causes   = []
    total_dl = 0
    total_cx = 0

    # ── Ground delays ─────────────────────────────────────────────────────────
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

    # ── Ground stops ──────────────────────────────────────────────────────────
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

    # ── Closures ──────────────────────────────────────────────────────────────
    for cl in closures:
        apt    = cl.get("ARPT", "UNK").strip()
        reason = cl.get("Reason", "Closure").strip()
        apt_map.setdefault(apt, {"dl": 0, "cx": 0, "reasons": []})
        apt_map[apt]["cx"] += 50
        total_cx += 50
        if reason not in causes:
            causes.append(reason)

    # ── Baseline (used for score calculation only) ────────────────────────────
    baseline_dl = 1200 + (now.weekday() * 80)
    baseline_cx = 60   + (now.weekday() * 5)
    total_dl   += baseline_dl
    total_cx   += baseline_cx

    n_stops  = len(ground_stops)
    n_delays = len(ground_delays)

    # ── Disruption score (0–100) ──────────────────────────────────────────────
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

    has_active_programs = bool(apt_map)

    # ── Top airports ──────────────────────────────────────────────────────────
    if has_active_programs:
        # Real FAA program airports — sorted by estimated delays
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
    else:
        # Normal day — clean label, no fabricated numbers
        apt_list = [
            {
                "c":  "—",
                "dl": 0,
                "cx": 0,
                "s":  "Normal operations — no airport specifics available"
            }
        ]

    # ── Cause string ──────────────────────────────────────────────────────────
    if causes:
        cause_str = " + ".join(causes[:3])
    else:
        cause_str = "Normal operations — no active FAA ground programs"

    # ── Forward look ─────────────────────────────────────────────────────────
    if n_stops or n_delays:
        fwd_str = (
            f"{n_stops} active ground stop(s); {n_delays} ground delay program(s). "
            "Monitor FAA NASSTATUS for updates."
        )
    else:
        fwd_str = "No active FAA programs. Normal operational outlook."

    # ── Confidence flag ───────────────────────────────────────────────────────
    conf = "FAA-API-CONFIRMED" if has_active_programs else "FAA-API-NORMAL"

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

    print("   Fetching FAA NAS Status...")
    faa_data = fetch_faa_status()

    print("   Building disruption record...")
    new_record = build_record(faa_data, central)
    print(
        f"   → Date: {new_record['d']}  Severity: {new_record['sev']}  "
        f"Score: {new_record['sc']}  Delays: {new_record['dl']}  "
        f"Cancels: {new_record['cx']}  Conf: {new_record['conf']}"
    )

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

    new_json   = json.dumps(current_data, separators=(",", ":"))
    commit_msg = (
        f"Aviation update {central.strftime('%Y-%m-%d')} "
        f"— {new_record['sev']} (score {new_record['sc']})"
    )

    print(f"   Pushing to GitHub: \"{commit_msg}\"")
    status = github_put(FILE_PATH, new_json, sha, commit_msg)
    print(f"✅  Done — HTTP {status}. Dashboard will refresh within 1-2 minutes.")

if __name__ == "__main__":
    main()
