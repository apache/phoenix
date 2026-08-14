#!/usr/bin/env python3
"""Phoenix HA metric post-processing.

Usage: analyze_phoenix_ha_metrics.py [data_dir] [--threshold N]

Designed for the JSON dump produced by the phoenix-ha-metrics extractor:
30 series (10 metrics * {median, p99, max, numops}) keyed by `device` tag.

Notes on counter handling:
1. phxwalsync_numops oscillates between cumulative-real values and 0/1 stale
   readings (likely two metric-source instances reporting under the same
   `device` label). We forward-delta only over values >= --threshold, which
   filters out the 0/1 noise. If a future run has tiny windows where
   legitimate cumulative counts stay below the default 100, raise it via
   --threshold; the diagnostic header will warn if any pod has only filtered
   noise.
2. Histogram value series are sparse (many zero entries even when counter
   shows real activity). nonzero_mean treats 0 as "no sample reported in
   this window" rather than "p99 was 0ms."
3. Cluster aggregates use simple mean across active pods, NOT ops-weighted
   means -- weights are themselves corrupt and per-pod sample counts are too
   low (~10 real histogram buckets per pod over ~27 scrapes) for weighting
   to add precision.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# CLI

ap = argparse.ArgumentParser()
ap.add_argument("data_dir", nargs="?", default=".",
                help="directory containing the 30 JSON files")
ap.add_argument("--threshold", type=int, default=100,
                help="phxwalsync_numops values below this are treated as "
                     "stale/zero noise (default: 100)")
args = ap.parse_args()

DIR = Path(args.data_dir)
NS_TO_MS = 1e6
LARGE_THRESHOLD = args.threshold

if not DIR.is_dir():
    sys.exit(f"data_dir not found: {DIR}")


# ---------------------------------------------------------------------------
# Helpers

def by_pod(arr):
    return {s["tags"]["device"]: {int(ts): v for ts, v in s["datapoints"].items()}
            for s in arr}


def filtered_delta(series, threshold=LARGE_THRESHOLD):
    """Forward-delta over the subseries of values >= threshold.
    Filters out 0/1 stale-counter noise from phxwalsync_numops."""
    items = sorted(series.items())
    large = [(ts, v) for ts, v in items if v >= threshold]
    if len(large) < 2:
        return 0.0
    total = 0.0
    prev = large[0][1]
    for _, v in large[1:]:
        d = v - prev
        if d >= 0:
            total += d
        prev = v
    return total


def nonzero_mean(series, scale=1.0):
    """Mean of non-zero values, scaled. Treats 0 as 'no sample'."""
    vals = [v / scale for v in series.values() if v > 0]
    return sum(vals) / len(vals) if vals else None


def nonzero_max(series, scale=1.0):
    vals = [v / scale for v in series.values() if v > 0]
    return max(vals) if vals else None


def fmt(v, unit="ms"):
    if v is None:
        return "n/a"
    if unit == "count":
        return f"{v:.1f}"
    if unit == "ms" and abs(v) < 0.1 and v != 0:
        return f"{v*1000:.0f} µs"
    if abs(v) >= 100:
        return f"{v:.0f} ms"
    return f"{v:.2f} ms"


# ---------------------------------------------------------------------------
# Load

def load(name):
    return by_pod(json.load(open(DIR / f"{name}.json")))


REQUIRED = [
    "replsync_median", "replsync_p99", "replsync_max", "replsync_numops",
    "synctime_median", "synctime_p99", "synctime_max", "synctime_numops",
    "phxwalsync_median", "phxwalsync_p99", "phxwalsync_max", "phxwalsync_numops",
    "phxwalappend_p99", "phxwalappend_max", "phxwalappend_numops",
    "ringbuf_median", "ringbuf_p99", "ringbuf_numops",
    "fssync_median", "fssync_p99", "fssync_max",
    "pendwait_median", "pendwait_p99", "pendwait_max",
    "pendcount_median", "pendcount_p99", "pendcount_max",
    "batchsize_median", "batchsize_p99", "batchsize_max",
]
missing = [k for k in REQUIRED if not (DIR / f"{k}.json").exists()]
if missing:
    sys.exit(f"missing required JSON files in {DIR}: {missing}")

m = {k: load(k) for k in REQUIRED}


# ---------------------------------------------------------------------------
# Window detection from data (min/max ts across all loaded series)

all_ts = []
for series_by_pod in m.values():
    for ts_map in series_by_pod.values():
        all_ts.extend(ts_map.keys())
if not all_ts:
    sys.exit("no datapoints in any loaded series")
WINDOW_START_MS = min(all_ts)
WINDOW_END_MS = max(all_ts)
WINDOW_S = (WINDOW_END_MS - WINDOW_START_MS) / 1000

start_str = datetime.fromtimestamp(WINDOW_START_MS / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
end_str = datetime.fromtimestamp(WINDOW_END_MS / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------------
# Pod set

all_pods = sorted({p for v in m.values() for p in v}
                  - {p for v in m.values() for p in v if p.startswith("regionserver-sys")})


# ---------------------------------------------------------------------------
# Activity assessment

phx_delta = {p: filtered_delta(m["phxwalsync_numops"].get(p, {})) for p in all_pods}
repl_delta = {p: filtered_delta(m["replsync_numops"].get(p, {}), threshold=10) for p in all_pods}
hbase_delta = {p: filtered_delta(m["synctime_numops"].get(p, {}), threshold=10) for p in all_pods}

active = sorted([p for p in all_pods if phx_delta[p] > 0],
                key=lambda p: -phx_delta[p])


# ---------------------------------------------------------------------------
# Diagnostic header (run this first; it surfaces threshold mismatches)

print("=" * 88)
print(f"Phoenix HA write-path metrics  (analyze_phoenix_ha_metrics.py)")
print(f"Data dir:  {DIR}")
print(f"Window:    {start_str} -> {end_str}  ({WINDOW_S/3600:.2f} h)")
print(f"Threshold: phxwalsync_numops values < {LARGE_THRESHOLD} are treated as noise")
print("=" * 88)
print()
print("DIAGNOSTIC: histogram sparsity + counter shape per pod")
print("-" * 88)
print(f"{'Pod':<22} {'phxsync_p99':>16} {'phxsync_numops':>22} {'classified':>14}")
print(f"{'':<22} {'(nonzero/total)':>16} {'(min,max,n_large)':>22} {'':>14}")
print("-" * 88)
for p in sorted(all_pods, key=lambda p: -phx_delta[p]):
    p99 = m["phxwalsync_p99"].get(p, {})
    n_total = len(p99)
    n_nonzero = sum(1 for v in p99.values() if v > 0)
    nums = m["phxwalsync_numops"].get(p, {}).values()
    if nums:
        nmin = min(nums)
        nmax = max(nums)
        n_large = sum(1 for v in nums if v >= LARGE_THRESHOLD)
    else:
        nmin = nmax = n_large = 0
    if phx_delta[p] > 0:
        cls = "active"
    elif n_large > 0:
        cls = "below-thresh?"
    elif hbase_delta[p] > 0:
        cls = "no-phx-traffic"
    else:
        cls = "idle"
    sparsity = f"{n_nonzero}/{n_total}"
    counter = f"({nmin:.0f}, {nmax:.0f}, {n_large})"
    print(f"{p:<22} {sparsity:>16} {counter:>22} {cls:>14}")

# Surface a warning if any pod has cumulative counter activity but no value
# above threshold; that suggests --threshold is too high for this dataset.
suspicious = [p for p in all_pods if phx_delta[p] == 0
              and any(v >= 10 and v < LARGE_THRESHOLD for v in m["phxwalsync_numops"].get(p, {}).values())
              and any(v > 0 for v in m["phxwalsync_p99"].get(p, {}).values())]
if suspicious:
    print()
    print(f"WARNING: {len(suspicious)} pod(s) have phxwalsync_numops in [10, {LARGE_THRESHOLD}) "
          f"and non-zero histogram values, but were classified inactive. "
          f"Lower --threshold if this is a small-window run: {suspicious}")
print()
print(f"Active pods (real Phoenix WAL replication in window): {len(active)}")
print()


# ---------------------------------------------------------------------------
# v1 vs v2 activity comparison (kept as a sanity check on the threshold filter)

def naive_delta(series):
    items = sorted(series.items())
    if len(items) < 2:
        return 0
    total = 0.0
    prev = items[0][1]
    for _, v in items[1:]:
        d = v - prev
        if d >= 0:
            total += d
        prev = v
    return total


print("=" * 88)
print("ACTIVITY: naive forward-delta vs threshold-filtered delta")
print("=" * 88)
print(f"{'Pod':<22} {'phxWAL Δ (naive)':>18} {'phxWAL Δ (filt)':>17} "
      f"{'replSync Δ':>12} {'HBase Δ':>12}")
print("-" * 88)
for p in sorted(all_pods, key=lambda p: -phx_delta[p]):
    naive = naive_delta(m["phxwalsync_numops"].get(p, {}))
    filt = phx_delta[p]
    if naive == 0 and filt == 0 and repl_delta[p] == 0 and hbase_delta[p] == 0:
        continue
    print(f"{p:<22} {naive:>18,.0f} {filt:>17,.0f} {repl_delta[p]:>12,.0f} {hbase_delta[p]:>12,.0f}")
print()


# ---------------------------------------------------------------------------
# Per-pod stats

def pod_metric_stats(pod, med_key, p99_key, max_key, scale=1.0):
    s50 = nonzero_mean(m.get(med_key, {}).get(pod, {}), scale) if med_key else None
    s99 = nonzero_mean(m.get(p99_key, {}).get(pod, {}), scale) if p99_key else None
    smx = nonzero_max(m.get(max_key, {}).get(pod, {}), scale) if max_key else None
    return s50, s99, smx


metrics_def = {
    "ReplicationSyncTime":   ("replsync_median",   "replsync_p99",      "replsync_max",      1.0),
    "HBaseSyncTime":         ("synctime_median",   "synctime_p99",      "synctime_max",      1.0),
    "PhoenixWALSyncTimeMs":  ("phxwalsync_median", "phxwalsync_p99",    "phxwalsync_max",    1.0),
    "AppendTime_ms":         (None,                "phxwalappend_p99",  "phxwalappend_max",  NS_TO_MS),
    "FsSyncTime":            ("fssync_median",     "fssync_p99",        "fssync_max",        1.0),
    "PendingSyncWait_ms":    ("pendwait_median",   "pendwait_p99",      "pendwait_max",      NS_TO_MS),
    "RingBuffer_ms":         ("ringbuf_median",    "ringbuf_p99",       None,                NS_TO_MS),
    "PendingSyncCount":      ("pendcount_median",  "pendcount_p99",     "pendcount_max",     1.0),
    "BatchSize":             ("batchsize_median",  "batchsize_p99",     "batchsize_max",     1.0),
}

pod_stats = {n: {p: pod_metric_stats(p, *args) for p in active}
             for n, args in metrics_def.items()}


def cluster_mean(name, idx):
    vals = [pod_stats[name][p][idx] for p in active if pod_stats[name][p][idx] is not None]
    return sum(vals) / len(vals) if vals else None


def cluster_max(name):
    vals = [pod_stats[name][p][2] for p in active if pod_stats[name][p][2] is not None]
    return max(vals) if vals else None


labels = {
    "ReplicationSyncTime":  ("ReplicationSyncTime",          "ms"),
    "HBaseSyncTime":        ("HBase WAL SyncTime",           "ms"),
    "PhoenixWALSyncTimeMs": ("Phoenix WAL SyncTime",         "ms"),
    "AppendTime_ms":        ("AppendTime (ns->ms)",          "ms"),
    "FsSyncTime":           ("FsSyncTime",                   "ms"),
    "PendingSyncWait_ms":   ("PendingSyncWaitTime (ns->ms)", "ms"),
    "RingBuffer_ms":        ("RingBufferTime (ns->ms)",      "ms"),
    "PendingSyncCount":     ("PendingSyncCount",             "count"),
    "BatchSize":            ("BatchSize",                    "count"),
}


# ---------------------------------------------------------------------------
# Cluster summary

if not active:
    sys.exit("no active pods after filtering -- check --threshold or the data")

print("=" * 88)
print("CLUSTER SUMMARY  (simple mean across active pods of per-pod nonzero-mean)")
print("Note: ReplicationSyncTime is per-table; all other metrics are HA-group-wide")
print("=" * 88)
print(f"{'Metric':<32} {'~p50':>10} {'~p99':>10} {'max':>10}  {'unit':<5}  {'N pods':>6}")
print("-" * 88)
for name, (lab, unit) in labels.items():
    p50 = cluster_mean(name, 0)
    p99 = cluster_mean(name, 1)
    mx = cluster_max(name)
    n_pods = sum(1 for p in active if pod_stats[name][p][1] is not None)
    p50s = "-" if name == "AppendTime_ms" else fmt(p50, unit)
    mxs = "-" if name == "RingBuffer_ms" else fmt(mx, unit)
    print(f"{lab:<32} {p50s:>10} {fmt(p99, unit):>10} {mxs:>10}  {unit:<5}  {n_pods:>6}")
print()


# ---------------------------------------------------------------------------
# Per-RS decomposition for the 5 busiest

print("=" * 88)
print("PER-RS DECOMPOSITION  (5 busiest by filtered phxWAL ops)")
print("=" * 88)
busy = active[:5]
for p in busy:
    rate = phx_delta[p] / WINDOW_S
    print(f"\n{p}   ops/sec={rate:,.1f}   sync_count={int(phx_delta[p]):,}   "
          f"replSync_count={int(repl_delta[p]):,}")
    print(f"{'Component':<32} {'p50':>10} {'p99':>10} {'max':>10}")
    print("-" * 64)
    rb50, rb99, _ = pod_stats["RingBuffer_ms"][p]
    pw50, pw99, pwmx = pod_stats["PendingSyncWait_ms"][p]
    fs50, fs99, fsmx = pod_stats["FsSyncTime"][p]
    ph50, ph99, phmx = pod_stats["PhoenixWALSyncTimeMs"][p]
    print(f"{'RingBufferTime':<32} {fmt(rb50):>10} {fmt(rb99):>10} {'-':>10}")
    print(f"{'PendingSyncWaitTime':<32} {fmt(pw50):>10} {fmt(pw99):>10} {fmt(pwmx):>10}")
    print(f"{'FsSyncTime':<32} {fmt(fs50):>10} {fmt(fs99):>10} {fmt(fsmx):>10}")
    rhs50 = (rb50 or 0) + (pw50 or 0) + (fs50 or 0)
    rhs99 = (rb99 or 0) + (pw99 or 0) + (fs99 or 0)
    print(f"{'  sum (RHS)':<32} {fmt(rhs50):>10} {fmt(rhs99):>10} {'-':>10}")
    print(f"{'Phoenix WAL SyncTime (LHS)':<32} {fmt(ph50):>10} {fmt(ph99):>10} {fmt(phmx):>10}")
    if ph50:
        r50 = (ph50 - rhs50) / ph50 * 100
        print(f"{'  residual at p50':<32} {r50:+.1f}%")
    if ph99:
        r99 = (ph99 - rhs99) / ph99 * 100
        print(f"{'  residual at p99':<32} {r99:+.1f}%")
    if ph50:
        print(f"  fractions p50: RB={(rb50 or 0)/ph50*100:.0f}%  "
              f"PW={(pw50 or 0)/ph50*100:.0f}%  Fs={(fs50 or 0)/ph50*100:.0f}%")
    if ph99:
        print(f"  fractions p99: RB={(rb99 or 0)/ph99*100:.0f}%  "
              f"PW={(pw99 or 0)/ph99*100:.0f}%  Fs={(fs99 or 0)/ph99*100:.0f}%")


# ---------------------------------------------------------------------------
# Phoenix vs HBase HDFS comparison

print()
print("=" * 88)
print("PHOENIX vs HBASE HDFS-LAYER COMPARISON  (same RS, same window)")
print("=" * 88)
print(f"{'Pod':<22} {'PhxFsSync p50':>14} {'HBase Sync p50':>16} {'delta':>8}   "
      f"{'PhxFsSync p99':>14} {'HBase Sync p99':>16} {'delta':>8}")
print("-" * 110)
for p in busy:
    fs50, fs99, _ = pod_stats["FsSyncTime"][p]
    hb50, hb99, _ = pod_stats["HBaseSyncTime"][p]
    d50 = f"{(fs50-hb50)/hb50*100:+.0f}%" if (fs50 and hb50) else "-"
    d99 = f"{(fs99-hb99)/hb99*100:+.0f}%" if (fs99 and hb99) else "-"
    print(f"{p:<22} {fmt(fs50):>14} {fmt(hb50):>16} {d50:>8}   "
          f"{fmt(fs99):>14} {fmt(hb99):>16} {d99:>8}")
print()


# ---------------------------------------------------------------------------
# Load-regime characterization

print("=" * 88)
print("LOAD-REGIME CHARACTERIZATION")
print("=" * 88)
rates = sorted([phx_delta[p] / WINDOW_S for p in active])
print(f"Caller-sync rate per RS:  min={rates[0]:,.1f}/s  median={rates[len(rates)//2]:,.1f}/s  max={rates[-1]:,.1f}/s")

pc99 = cluster_mean("PendingSyncCount", 1)
pc_worst = max(active, key=lambda p: (pod_stats["PendingSyncCount"][p][1] or 0))
print(f"PendingSyncCount p99:     cluster mean={pc99:.2f}  worst RS={pod_stats['PendingSyncCount'][pc_worst][1]:.1f} on {pc_worst}")

bs99 = cluster_mean("BatchSize", 1)
bs50 = cluster_mean("BatchSize", 0)
bs_worst = max(active, key=lambda p: (pod_stats["BatchSize"][p][1] or 0))
print(f"BatchSize:                cluster ~p50={bs50:.1f}  cluster ~p99={bs99:.1f}  "
      f"worst RS p99={pod_stats['BatchSize'][bs_worst][1]:.1f} on {bs_worst}")

print(f"\nCoalescing engaging?      {'Yes' if pc99 and pc99 >= 10 else 'No '} "
      f"(PendingSyncCount p99 cluster = {pc99:.2f})")
if bs99 and bs50:
    print(f"Consumer in catch-up?     {'Yes' if bs99 > 3*bs50 else 'No '} (p99/p50 = {bs99/bs50:.1f}x)")
print()


# ---------------------------------------------------------------------------
# Per-RS detail for representative pods

print("=" * 88)
print("PER-REGIONSERVER DETAIL  (representative pods)")
print("=" * 88)
candidates = [
    ("highest ops",       active[0]),
    ("median ops",        active[len(active)//2]),
    ("lowest active",     active[-1]),
    ("worst syncTime p99", max(active, key=lambda p: (pod_stats["PhoenixWALSyncTimeMs"][p][1] or 0))),
    ("worst syncTime max", max(active, key=lambda p: (pod_stats["PhoenixWALSyncTimeMs"][p][2] or 0))),
]
seen = set()
for role, p in candidates:
    if p in seen:
        continue
    seen.add(p)
    print(f"\n{role}: {p}    phxWAL_ops={int(phx_delta[p]):,}    repl_ops={int(repl_delta[p]):,}")
    print(f"{'Metric':<32} {'p50':>10} {'p99':>10} {'max':>10}")
    print("-" * 64)
    for name, (lab, unit) in labels.items():
        p50, p99, mx = pod_stats[name][p]
        p50s = "-" if name == "AppendTime_ms" else fmt(p50, unit)
        mxs = "-" if name == "RingBuffer_ms" else fmt(mx, unit)
        print(f"{lab:<32} {p50s:>10} {fmt(p99, unit):>10} {mxs:>10}")
print()


# ---------------------------------------------------------------------------
# Residual-gap conclusion

ph99 = cluster_mean("PhoenixWALSyncTimeMs", 1)
rb99 = cluster_mean("RingBuffer_ms", 1)
pw99 = cluster_mean("PendingSyncWait_ms", 1)
fs99 = cluster_mean("FsSyncTime", 1)
ph50 = cluster_mean("PhoenixWALSyncTimeMs", 0)
rb50 = cluster_mean("RingBuffer_ms", 0)
pw50 = cluster_mean("PendingSyncWait_ms", 0)
fs50 = cluster_mean("FsSyncTime", 0)
print("=" * 88)
print("RESIDUAL-GAP CONCLUSION  (cluster mean across active pods)")
print("=" * 88)
if ph99:
    print(f"At p99:")
    print(f"  RingBuffer={rb99/ph99*100:.0f}%  Fs={fs99/ph99*100:.0f}%  "
          f"PW={pw99/ph99*100:.0f}%   "
          f"closure={(ph99-(rb99+pw99+fs99))/ph99*100:+.0f}%")
if ph50:
    print(f"At p50:")
    print(f"  RingBuffer={rb50/ph50*100:.0f}%  Fs={fs50/ph50*100:.0f}%  "
          f"PW={pw50/ph50*100:.0f}%   "
          f"closure={(ph50-(rb50+pw50+fs50))/ph50*100:+.0f}%")
if ph99:
    v2_keep = (fs99/ph99 + pw99/ph99 + (rb99/ph99)/5) * 100
    print(f"\nv2 (5x ringBuffer reduction) leaves ~{v2_keep:.0f}% of current p99 syncTime")
print()
