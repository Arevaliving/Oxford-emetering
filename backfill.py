#!/usr/bin/env python3
"""
Back-fill daily_history.json from all historical CSV files on SFTP.
Extracts the latest cumulative reading per meter serial per day.
"""
import json, sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, '/home/oxford/oxford-sync')
import paramiko

SFTP_HOST = "82.25.83.153"
SFTP_PORT = 65002
SFTP_USER = "u382481972"
SFTP_PASS = "daFQ_iHm9bASWJ#"
REMOTE_DIR = "/home/u382481972/meter_uploads/"
DAILY_JSON = Path("/home/oxford/oxford-sync/www/data/daily_history.json")

def load_history():
    if DAILY_JSON.exists():
        return json.loads(DAILY_JSON.read_text())
    return {}

def save_history(h):
    DAILY_JSON.parent.mkdir(parents=True, exist_ok=True)
    DAILY_JSON.write_text(json.dumps(h, separators=(',', ':')))

def date_from_filename(fname):
    # e.g. 0016044252_valuereport_20260218154938_2112.csv -> 2026-02-18
    try:
        ts = fname.split('_valuereport_')[1][:8]
        return datetime.strptime(ts, '%Y%m%d').date().isoformat()
    except Exception:
        return None

def parse_csv_latest_per_serial(content):
    """Return {serial: cumulative_m3} for the latest row per serial."""
    records = {}
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split(';')
        if len(parts) < 21:
            continue
        serial = parts[2].strip()
        ts     = parts[3].strip()
        try:
            current = float(parts[13].strip())
        except (ValueError, IndexError):
            continue
        if current <= 0:
            continue
        # Keep latest timestamp per serial
        if serial not in records or ts > records[serial]['ts']:
            records[serial] = {'ts': ts, 'cur': current}
    return {s: v['cur'] for s, v in records.items()}

def main():
    print("Connecting to SFTP...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASS)
    sftp = ssh.open_sftp()

    files = sorted(sftp.listdir(REMOTE_DIR))
    print(f"Found {len(files)} files")

    history = load_history()
    updated = 0

    for fname in files:
        date_str = date_from_filename(fname)
        if not date_str:
            print(f"  SKIP (no date): {fname}")
            continue

        print(f"  Processing {fname} -> {date_str}")
        with sftp.open(REMOTE_DIR + fname, 'r') as fh:
            content = fh.read().decode('utf-8-sig', errors='replace')

        readings = parse_csv_latest_per_serial(content)
        for serial, cum in readings.items():
            if serial not in history:
                history[serial] = {}
            # Only set if not already present (don't overwrite a later reading)
            if date_str not in history[serial]:
                history[serial][date_str] = cum
                updated += 1
            # But if this file is newer (later timestamp in filename), prefer it
            # Since files are sorted, later files win for the same date
            else:
                history[serial][date_str] = cum

    sftp.close()
    ssh.close()

    save_history(history)
    serials = len(history)
    total_entries = sum(len(v) for v in history.values())
    print(f"\nDone. {serials} serials, {total_entries} total date entries saved.")
    print(f"Sample dates for first serial: {sorted(list(history.values())[0].keys())}")

if __name__ == '__main__':
    main()
