#!/usr/bin/env python3
import paramiko,io,json
from pathlib import Path
from datetime import datetime
HOST="82.25.83.153";PORT=65002;USER="u382481972";PASS="daFQ_iHm9bASWJ#"
RPATH="/home/u382481972/meter_uploads/Historical data"
DJSON=Path("/home/oxford/oxford-sync/www/data/daily_history.json")

def parse(text):
    out={}
    for line in text.splitlines():
        line=line.strip()
        if line.startswith("$") or not line: continue
        p=line.split(";")
        if len(p)<21: continue
        serial=p[2].strip()
        ts=p[3].strip()
        val=p[13].strip().replace(",",".")
        if not serial or not ts or not val: continue
        try:
            dt=datetime.strptime(ts[:10],"%Y-%m-%d")
            m3=float(val)
        except: continue
        d=dt.strftime("%Y-%m-%d")
        if serial not in out: out[serial]={}
        if d not in out[serial] or m3>out[serial][d]: out[serial][d]=m3
    return out

def main():
    hist=json.loads(DJSON.read_text()) if DJSON.exists() else {}
    print(f"Existing: {len(hist)} serials, {sum(len(v) for v in hist.values())} entries")
    tr=paramiko.Transport((HOST,PORT))
    tr.connect(username=USER,password=PASS)
    sf=paramiko.SFTPClient.from_transport(tr)
    files=sorted(sf.listdir(RPATH))
    print(f"Historical files: {len(files)}")
    new_d=0; new_s=0
    for fn in files:
        buf=io.BytesIO(); sf.getfo(RPATH+"/"+fn,buf)
        text=buf.getvalue().decode("utf-8-sig","replace")
        parsed=parse(text)
        cnt=0
        for ser,dates in parsed.items():
            if ser not in hist: hist[ser]={}; new_s+=1
            for d,m3 in dates.items():
                if d not in hist[ser]: hist[ser][d]=m3; new_d+=1; cnt+=1
        if cnt: print(f"  {fn}: +{cnt}")
    sf.close(); tr.close()
    DJSON.write_text(json.dumps(hist))
    print(f"Done: +{new_d} entries, +{new_s} serials")
    print(f"Total: {len(hist)} serials, {sum(len(v) for v in hist.values())} entries")
if __name__=="__main__": main()
