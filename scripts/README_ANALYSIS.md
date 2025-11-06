# from repo root
export PYTHONPATH=src

## Testing UDP
```shell
uv run scripts/client.py
```

```shell
uv run scripts/server.py
```

Network modes
```shell
# Good
sudo tc qdisc add dev lo root netem delay 50ms 10ms loss 1%

# Average
sudo tc qdisc add dev lo root netem delay 200ms 25ms loss 5%

# Poor
sudo tc qdisc add dev lo root netem delay 500ms 50ms loss 15%
```

# create venv in WSL home (fast)
python3 -m venv ~/.venvs/ass4
source ~/.venvs/ass4/bin/activate
python -m pip install --upgrade pip
python -m pip install matplotlib

- Baseline UDP
# terminal A - server
python scripts/baseline_udp_echo_server.py 

# terminal B - client
export PYTHONPATH=src
rm -f metrics.csv
python scripts/analyse_client.py --mode udp --addr 127.0.0.1 --port 9000 --pps 300 --duration 10 --payload 64
mv -f metrics.csv metrics_udp_clean.csv

- HUDP
# terminal A
Ctrl+C   # stop baseline first
export PYTHONPATH=src
python scripts/hudp_echo_server.py            

# terminal B
export PYTHONPATH=src
rm -f metrics.csv
python scripts/analyse_client.py --mode hudp --addr 127.0.0.1 --port 9999 --pps 300 --duration 10 --payload 64
mv -f metrics.csv metrics_hudp_clean.csv

# plots
# HUDP plots
cp -f metrics_hudp_clean.csv metrics.csv
python scripts/plot_metrics.py

# UDP plots
cp -f metrics_udp_clean.csv  metrics.csv
python scripts/plot_metrics.py

# quick summary
python - <<'PY'
import csv, math, statistics as S, sys
def pct(a,p): a=sorted(a); k=(len(a)-1)*p/100; f=math.floor(k); c=min(f+1,len(a)-1); \
    print("", end=""); return a[f] if f==k else a[f]+(a[c]-a[f])*(k-f)
for fn in ("metrics_udp_clean.csv","metrics_hudp_clean.csv"):
    rows=[r for r in csv.DictReader(open(fn)) if r["event"]=="echo_ok"]
    mode = rows[0]["mode"] if rows else fn
    rtt=[float(r["rtt_ms"]) for r in rows if r["rtt_ms"]]
    jit=[float(r["jitter_ms"]) for r in rows if r["jitter_ms"]]
    sent = max((int(r["seq"]) for r in csv.DictReader(open(fn))), default=-1)+1
    got  = len(rows); pdr = got/sent if sent>0 else 0.0
    print(f"\n=== {fn} ({mode}) ===")
    if rtt: print(f"RTT ms: avg {S.mean(rtt):.3f}  p50 {pct(rtt,50):.3f}  p95 {pct(rtt,95):.3f}")
    if jit: print(f"Jitter ms: avg {S.mean(jit):.3f}")
    print(f"PDR: {pdr*100:.2f}%  ({got}/{sent})")
PY
