#!/usr/bin/env python3
"""
Oxford Suites — Enerpro SFTP Sync
===================================
Pulls both gateway CSVs, merges via meter list, writes latest.json.

Retry logic:
  - Runs every 2 hours via cron (00:30, 02:30, 04:30, 06:30)
  - If by 08:00 new data still not available → writes stale flag
    so dashboard shows last known data with a warning banner.

Usage:
  python3 sync.py              # normal run
  python3 sync.py --force      # ignore date check, always write JSON
"""

import os, sys, json, logging, shutil, hashlib
from datetime import datetime, date, timedelta
from pathlib import Path
import paramiko                 # pip install paramiko
import pandas as pd             # pip install pandas openpyxl

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
SFTP_HOST     = "82.25.83.153"
SFTP_PORT     = 65002
SFTP_USER     = "u382481972"
SFTP_PASS     = "daFQ_iHm9bASWJ#"
SFTP_KEY_PATH = ""                        # not used — password auth
SFTP_REMOTE_DIR = "/meter_uploads/"      # will probe /home/u382481972/meter_uploads if this fails

GATEWAY_WEST  = "0016046347"              # 63 units
GATEWAY_EAST  = "0016044252"             # 77 units

BASE_DIR      = Path(__file__).parent
METER_LIST    = BASE_DIR / "Suite-Meter_List_Oxford.xlsx"
ARCHIVE_DIR   = BASE_DIR / "archive"
OUTPUT_JSON   = BASE_DIR / "www" / "data" / "latest.json"
LOG_FILE      = BASE_DIR / "sync.log"

# If no new data by this hour (24h), write stale flag
STALE_CUTOFF_HOUR = 8   # 08:00 local time
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def connect_sftp():
    """Open SFTP connection. Supports password or key auth."""
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    if SFTP_KEY_PATH:
        key = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
        transport.connect(username=SFTP_USER, pkey=key)
    else:
        transport.connect(username=SFTP_USER, password=SFTP_PASS)
    return paramiko.SFTPClient.from_transport(transport), transport


def resolve_remote_dir(sftp):
    """
    Probe candidate paths and return the first that is listable.
    Handles servers that expose full /home/user paths vs. those that
    drop the user directly into their home as the SFTP root.
    """
    candidates = [
        "/meter_uploads",
        "/home/u382481972/meter_uploads",
        "meter_uploads",
        ".",
    ]
    for path in candidates:
        try:
            entries = sftp.listdir(path)
            log.info(f"Remote directory resolved: {path!r}  ({len(entries)} entries found)")
            return path.rstrip("/") + "/"
        except (FileNotFoundError, IOError):
            continue
    log.warning("Could not resolve remote dir — falling back to configured SFTP_REMOTE_DIR")
    return SFTP_REMOTE_DIR


def list_remote_csvs(sftp, gateway_id, remote_dir):
    """
    Find CSV files on remote matching the gateway ID.
    Enerpro naming: {gatewayID}_valuereport_{timestamp}_{seq}.csv
    Returns sorted list (newest last).
    """
    try:
        files = sftp.listdir(remote_dir)
    except (FileNotFoundError, IOError) as e:
        log.error(f"Cannot list remote dir {remote_dir!r}: {e}")
        return []
    matching = sorted([
        f for f in files
        if f.startswith(gateway_id) and f.endswith(".csv")
    ])
    log.info(f"  Gateway {gateway_id}: {len(matching)} matching file(s)")
    return matching


def is_todays_file(filename):
    """
    Elvaco filename: {gw}_valuereport_{YYYYMMDD}HHMMSS_{seq}.csv
    Check if the embedded date matches today or yesterday
    (gateways sync ~02:00 so files may be timestamped yesterday).
    """
    try:
        ts_part = filename.split("_valuereport_")[1][:8]   # YYYYMMDD
        file_date = datetime.strptime(ts_part, "%Y%m%d").date()
        today = date.today()
        return file_date in (today, today - timedelta(days=1))
    except Exception:
        return False


def download_csv(sftp, filename, local_path):
    """Download file only if it has changed (hash check)."""
    remote_path = SFTP_REMOTE_DIR + filename
    tmp_path = local_path.with_suffix(".tmp")
    sftp.get(remote_path, str(tmp_path))

    # Skip if identical to what we already have
    if local_path.exists():
        existing_hash = hashlib.md5(local_path.read_bytes()).hexdigest()
        new_hash = hashlib.md5(tmp_path.read_bytes()).hexdigest()
        if existing_hash == new_hash:
            tmp_path.unlink()
            log.info(f"  {filename} — unchanged, skipped")
            return False

    tmp_path.rename(local_path)
    log.info(f"  {filename} — downloaded ({local_path.stat().st_size:,} bytes)")
    return True


def parse_elvaco_csv(path):
    """
    Parse Elvaco CMe3100 value report CSV.
    Semicolon-delimited. Key columns (0-indexed):
      [2]  meter serial
      [3]  reading timestamp
      [12] last reading timestamp
      [13] current cumulative m³
      [17] Jan 1 reading
      [19] Jan 31 reading
      [21] Feb reading
      [33] Sep 30
      [35] Oct 31
      [37] Nov 30
      [39] Dec 31
    Returns dict: serial → parsed row
    """
    records = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line.startswith("#") or not line:
                continue
            parts = line.split(";")
            if len(parts) < 21:
                continue
            serial = parts[2].strip()
            ts     = parts[3].strip()
            # Keep only the earliest reading per meter (most recent cumulative)
            if serial not in records or ts > records[serial]["ts"]:
                records[serial] = {"ts": ts, "parts": parts}

    return {k: v["parts"] for k, v in records.items()}


def safe_float(parts, idx):
    try:
        v = float(parts[idx])
        return round(v, 4) if v > 0 else 0.0
    except (IndexError, ValueError):
        return 0.0


def build_units(records_combined, meter_lookup):
    """
    Merge parsed meter records with meter→unit lookup.
    Returns list of unit dicts sorted by unit number.
    """
    unit_meters = {}

    for serial, parts in records_combined.items():
        meta = meter_lookup.get(serial)
        if not meta:
            log.warning(f"  Unknown serial {serial} — not in meter list")
            continue

        unit  = meta["unit"]
        mtype = meta["type"].lower()   # "dhw" or "dcw"

        current = safe_float(parts, 13)
        jan1    = safe_float(parts, 17)
        jan31   = safe_float(parts, 19)
        feb     = safe_float(parts, 21)
        sep30   = safe_float(parts, 33)
        oct31   = safe_float(parts, 35)
        nov30   = safe_float(parts, 37)
        dec31   = safe_float(parts, 39)

        hist = [
            round(sep30, 4),
            round(max(0.0, oct31 - sep30),  4),
            round(max(0.0, nov30 - oct31),  4),
            round(max(0.0, dec31 - nov30),  4),
            round(max(0.0, jan31 - jan1),   4),
            round(max(0.0, current - jan31), 4),
        ]

        lr = parts[12][:16] if len(parts) > 12 else "N/A"

        if unit not in unit_meters:
            unit_meters[unit] = {}

        unit_meters[unit][mtype] = {
            "s":   serial,
            "cur": current,
            "hist": hist,
            "lr":  lr,
        }

    result = []
    for unit in sorted(unit_meters.keys(), key=lambda x: int(x)):
        m   = unit_meters[unit]
        dhw = m.get("dhw", {})
        dcw = m.get("dcw", {})
        result.append({
            "u":      unit,
            "f":      int(unit[0]),
            "dh":     dhw.get("hist", [0]*6),
            "dc":     dcw.get("hist", [0]*6),
            "dh_cur": dhw.get("cur", 0),
            "dc_cur": dcw.get("cur", 0),
            "dh_s":   dhw.get("s", ""),
            "dc_s":   dcw.get("s", ""),
            "lr":     dhw.get("lr") or dcw.get("lr", "N/A"),
        })

    return result


def load_meter_lookup():
    """Build serial → {unit, type} dict from the Excel correspondence table."""
    df = pd.read_excel(METER_LIST)
    lookup = {}
    for _, row in df.iterrows():
        try:
            serial = str(int(row["Meter Address"]))
            unit   = str(int(row["Unit"]))
            mtype  = str(row["Meter Type"]).strip()
            lookup[serial] = {"unit": unit, "type": mtype}
        except (ValueError, KeyError):
            continue
    log.info(f"Loaded meter list: {len(lookup)} entries")
    return lookup


def write_output(units, status, stale=False, stale_since=None, last_sync=None):
    """Write latest.json consumed by the React dashboard."""
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "status":        status,       # "ok" | "stale" | "error"
        "stale":         stale,
        "stale_since":   stale_since,  # ISO string or null
        "last_sync":     last_sync,    # last successful sync ISO string
        "unit_count":    len(units),
        "units":         units,
    }
    with open(OUTPUT_JSON, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    log.info(f"Written {OUTPUT_JSON} — {len(units)} units, status={status}")


def load_existing_json():
    """Read whatever latest.json exists on disk, or None."""
    if OUTPUT_JSON.exists():
        try:
            with open(OUTPUT_JSON) as f:
                return json.load(f)
        except Exception:
            pass
    return None


def run(force=False):
    log.info("=" * 60)
    log.info("Oxford SFTP Sync starting")

    # ── Load meter lookup ────────────────────────────────────────
    try:
        meter_lookup = load_meter_lookup()
    except Exception as e:
        log.error(f"Failed to load meter list: {e}")
        sys.exit(1)

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    # ── Connect SFTP ─────────────────────────────────────────────
    log.info(f"Connecting to {SFTP_HOST}:{SFTP_PORT} as {SFTP_USER}")
    try:
        sftp, transport = connect_sftp()
        log.info("SFTP connected")
        remote_dir = resolve_remote_dir(sftp)
    except Exception as e:
        log.error(f"SFTP connection failed: {e}")
        _handle_failure(meter_lookup)
        return

    try:
        csv_paths = {}

        for gw_id in (GATEWAY_WEST, GATEWAY_EAST):
            files = list_remote_csvs(sftp, gw_id, remote_dir)
            if not files:
                log.warning(f"No files found for gateway {gw_id}")
                continue

            # Latest file is the last in sorted list
            latest = files[-1]
            log.info(f"Gateway {gw_id}: latest file = {latest}")

            if not force and not is_todays_file(latest):
                log.warning(f"  {latest} is not from today/yesterday — skipping")
                continue

            local_path = ARCHIVE_DIR / latest
            try:
                changed = download_csv(sftp, latest, local_path)
            except Exception as e:
                log.error(f"  Download failed for {latest}: {e}")
                continue

            csv_paths[gw_id] = local_path

    finally:
        sftp.close()
        transport.close()
        log.info("SFTP disconnected")

    # ── Check we got both gateways ───────────────────────────────
    if len(csv_paths) < 2:
        missing = [g for g in (GATEWAY_WEST, GATEWAY_EAST) if g not in csv_paths]
        log.warning(f"Missing gateways: {missing}")
        _handle_failure(meter_lookup)
        return

    # ── Parse & merge ────────────────────────────────────────────
    log.info("Parsing CSVs...")
    try:
        records = {}
        for gw_id, path in csv_paths.items():
            parsed = parse_elvaco_csv(path)
            log.info(f"  {gw_id}: {len(parsed)} meter records")
            records.update(parsed)

        units = build_units(records, meter_lookup)
        log.info(f"Built {len(units)} unit records")
    except Exception as e:
        log.error(f"Parse error: {e}", exc_info=True)
        _handle_failure(meter_lookup)
        return

    # ── Write output ─────────────────────────────────────────────
    now_iso = datetime.utcnow().isoformat() + "Z"
    write_output(units, status="ok", stale=False, last_sync=now_iso)
    log.info("Sync complete ✓")


def _handle_failure(meter_lookup=None):
    """
    Called when sync fails. If it's past STALE_CUTOFF_HOUR and we
    have existing data, mark it stale. Otherwise do nothing (next
    cron attempt will try again).
    """
    now = datetime.now()
    existing = load_existing_json()

    if now.hour >= STALE_CUTOFF_HOUR:
        log.warning(
            f"Past {STALE_CUTOFF_HOUR:02d}:00 and no fresh data. "
            f"Marking as stale."
        )
        if existing and existing.get("units"):
            write_output(
                units        = existing["units"],
                status       = "stale",
                stale        = True,
                stale_since  = now.isoformat(),
                last_sync    = existing.get("last_sync"),
            )
        else:
            # No prior data at all — write empty stale payload
            write_output(
                units        = [],
                status       = "stale",
                stale        = True,
                stale_since  = now.isoformat(),
                last_sync    = None,
            )
    else:
        remaining = STALE_CUTOFF_HOUR - now.hour
        log.info(
            f"Sync failed but it's only {now.strftime('%H:%M')}. "
            f"Will retry — {remaining}h until stale cutoff."
        )


if __name__ == "__main__":
    force = "--force" in sys.argv
    run(force=force)
