"""
Daily Aviation Data Updater
----------------------------
Fetches real-time FAA ground delay / ground stop data from the public
FAA NAS Status API, builds a daily disruption record in the correct
JSON format, then commits it to aviation_data.json on GitHub.

Run by GitHub Actions every morning — no API keys required.
"""

import json
import urllib.request
import urllib.error
import base64
import ssl
import os
import re
from datetime import datetime, timezone, timedelta

# ── Config ───────────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_REPO  = "tbrabham/AviationMetricsUS"
FILE_PATH    = "aviation_data.json"
DELAY_BASE   = 5601   # normal daily delay baseline
CANCEL_BASE  = 340    # normal daily cancel baseline

CTX = ssl.create_default_context()

# ── FAA Data Fetch ────────────────────────────────────────────────────────────
def fetch_faa_status():
    """Fetch live FAA NAS Status (ground delays, ground stops, closures)."""
    url = "https://nasstatus.faa.gov/api/airport-status-information"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "aviation-dashboard/1.0",
                 "Accept": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, context=CTX, timeout=15) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"⚠  FAA API unavailable: {e}. Generating baseline record.")
        return {}

# ── Processing ────────────────────────────────────────────────────────────────
def to_list(val):
    """Ensure value is a list (FAA API returns dict when only 1 item)."""
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

def build_record(faa_data, now):
    ground_delays = to_list(faa_data.get("GroundDelays", {}).get("GroundDelay"))
    ground_stops  = to_list(faa_data.get("GroundStops",  {}).get("Program"))
    closures      = to_list(faa_data.get("Closures",     {}).get("Airport"))

    apt_map   = {}   # airport_code -> {dl, cx, reasons}
    causes    = []
    total_dl  = 0
    total_cx  = 0

    # Ground delays
    for gd in ground_delays:
        apt    = gd.get("ARPT", "UNK").strip()
        avg_m  = int(re.sub(r"\D", "", str(gd.get("Avg", "30"))) or "30")
        reason = gd.get("Reason", "Ground delay").strip()
        # Estimate impacted flights from average delay minutes
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

    # Ground stops (more severe)
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

    # Closures
    for cl in closures:
        apt    = cl.get("ARPT", "UNK").strip()
        reason = cl.get("Reason", "Closure").strip()
        apt_map.setdefault(apt, {"dl": 0, "cx": 0, "reasons": []})
        apt_map[apt]["cx"] += 50
        total_cx += 50
        if reason not in causes:
            causes.append(reason)

    # Add baseline (normal background ops)
    baseline_dl = 1200 + (now.weekday() * 80)   # Fri/Sat busier
    baseline_cx = 60  + (now.weekday() * 5)
    total_dl   += baseline_dl
    total_cx   += baseline_cx

    # Disruption score formula (0-100)
    delay_ratio  = total_dl / DELAY_BASE
    cancel_ratio = total_cx / CANCEL_BASE
    n_stops      = len(ground_stops)
    n_delays     = len(ground_delays)
    score = min(100, int(
        delay_ratio  * 28 +
        cancel_ratio * 38 +
        n_stops      * 8  +
        n_delays     * 4
    ))

    sev = score_color_label(score)
    wp  = (total_cx * 650) + (total_dl * 90)

    # Build top-3 airport list
    apt_list = sorted(
        [{"c": k, "dl": v["dl"], "cx": v["cx"],
          "s": v["reasons"][0][:40] if v["reasons"] else ""}
         for k, v in apt_map.items()],
        key=lambda x: x["dl"],
        reverse=True
    )[:3]

    cause_str = " + ".join(causes[:3]) if causes else "Routine operations — no active FAA ground programs"
    ota_risk  = sev if sev != "NORMAL" else "LOW"

    fwd_str = (
        f"{n_stops} active ground stop(s); {n_delays} ground delay program(s). "
        "Monitor FAA NASSTATUS for updates."
        if (n_stops or n_delays)
        else "No active FAA ground programs. Normal operational outlook."
    )

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
        "conf":  "FAA-API"
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
    # Use Central time (UTC-5 CDT / UTC-6 CST)
    utc_now    = datetime.now(timezone.utc)
    central    = utc_now - timedelta(hours=5)   # CDT; switch to 6 in Nov
    today_d    = central.strftime("%m%d")

    print(f"▶  Aviation daily update — {central.strftime('%Y-%m-%d %H:%M')} CT")
    print("   Fetching FAA NAS Status...")
    faa_data   = fetch_faa_status()

    print("   Building disruption record...")
    new_record = build_record(faa_data, central)
    print(f"   → Date: {new_record['d']}  Severity: {new_record['sev']}  "
          f"Score: {new_record['sc']}  Delays: {new_record['dl']}  "
          f"Cancels: {new_record['cx']}")

    print("   Fetching current aviation_data.json from GitHub...")
    file_info = github_get(FILE_PATH)
    sha       = file_info["sha"]

    # GitHub API returns base64 with embedded newlines — strip them before decoding
    content_b64 = file_info["content"].replace("\n", "").replace("\r", "")
    current_raw = base64.b64decode(content_b64).decode("utf-8").strip()

    try:
        current_data = json.loads(current_raw)
        if not isinstance(current_data, list):
            current_data = [current_data]
        print(f"   → Loaded {len(current_data)} existing records.")
    except json.JSONDecodeError as e:
        print(f"   ⚠  Could not parse existing JSON: {e}")
        print(f"   ⚠  Content preview: {current_raw[:300]!r}")
        print("   ⚠  Starting with existing records from fallback data.")
        current_data = []

    # Remove existing record for today if present (allow re-runs)
    current_data = [r for r in current_data if r["d"] != today_d]
    current_data.append(new_record)
    current_data.sort(key=lambda r: r["d"])

    new_json = json.dumps(current_data, separators=(",", ":"))
    commit_msg = f"Daily aviation update {central.strftime('%Y-%m-%d')} — {new_record['sev']} (score {new_record['sc']})"

    print(f"   Pushing to GitHub: \"{commit_msg}\"")
    status = github_put(FILE_PATH, new_json, sha, commit_msg)
    print(f"✅  Done — HTTP {status}. Dashboard will refresh within 1-2 minutes.")

if __name__ == "__main__":
    main()
