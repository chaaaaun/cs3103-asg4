# # scripts/plot_metrics.py
# import csv
# import matplotlib.pyplot as plt

# rows = []
# with open("metrics.csv", newline="") as f:
#     r = csv.DictReader(f)
#     for row in r:
#         rows.append(row)

# def series(filter_mode, key):
#     vals = []
#     for i, row in enumerate(rows):
#         if row["mode"] == filter_mode and row["event"] == "echo_ok" and row[key]:
#             vals.append((i, float(row[key])))
#     xs = [x for x,_ in vals]
#     ys = [y for _,y in vals]
#     return xs, ys

# for mode in ["udp-baseline", "hudp-unreliable"]:
#     xs, rtts = series(mode, "rtt_ms")
#     xs2, jit = series(mode, "jitter_ms")
#     xs3, thr = series(mode, "throughput_bps")

#     if rtts:
#         plt.figure(); plt.plot(xs, rtts, marker=".")
#         plt.title(f"{mode}: RTT (ms)"); plt.xlabel("packet"); plt.ylabel("ms"); plt.tight_layout()
#     if jit:
#         plt.figure(); plt.plot(xs2, jit)
#         plt.title(f"{mode}: Jitter (ms)"); plt.xlabel("packet"); plt.ylabel("ms"); plt.tight_layout()
#     if thr:
#         plt.figure(); plt.plot(xs3, thr)
#         plt.title(f"{mode}: Throughput (bps)"); plt.xlabel("sample"); plt.ylabel("bps"); plt.tight_layout()

# plt.show()

import csv, os
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt

INFILE = "metrics.csv"
OUTDIR = "plots"
os.makedirs(OUTDIR, exist_ok=True)

rows = []
with open(INFILE, newline="") as f:
    r = csv.DictReader(f)
    for row in r:
        rows.append(row)

def series(filter_mode, key):
    vals = []
    for i, row in enumerate(rows):
        if row["mode"] == filter_mode and row["event"] == "echo_ok" and row[key]:
            vals.append((i, float(row[key])))
    xs = [x for x,_ in vals]
    ys = [y for _,y in vals]
    return xs, ys

def save_plot(xs, ys, title, ylabel, fname):
    if not ys:
        print(f"[skip] {title} (no data)")
        return
    plt.figure()
    plt.plot(xs, ys, marker=".")
    plt.title(title)
    plt.xlabel("sample")
    plt.ylabel(ylabel)
    plt.tight_layout()
    path = os.path.join(OUTDIR, fname)
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"[saved] {path}")

modes = sorted({row["mode"] for row in rows if row.get("mode")})

for mode in modes:
    xs_rtt, rtts = series(mode, "rtt_ms")
    xs_jit, jit = series(mode, "jitter_ms")
    xs_thr, thr = series(mode, "throughput_bps")

    save_plot(xs_rtt, rtts, f"{mode}: RTT (ms)", "ms", f"{mode}_rtt.png")
    save_plot(xs_jit, jit, f"{mode}: Jitter (ms)", "ms", f"{mode}_jitter.png")
    save_plot(xs_thr, thr, f"{mode}: Throughput (bps)", "bps", f"{mode}_throughput.png")

print("[done] check the 'plots/' folder.")
