# HBase Compaction Strategy Analysis

## 1. How HBase Compaction Maps to LSM Compaction Strategies

HBase's default compaction is **size-tiered** (not level-based), with a few HBase-specific twists.

### Core Characterization

- **Size-tiered, per-Store**: Each Store (column family within a region) maintains a flat collection of HFiles. There are no levels in the LevelDB/RocksDB sense — all HFiles in a Store sit "side by side" and any subset can be selected for compaction.
- **Default policy**: `ExploringCompactionPolicy` (since ~0.96; previously `RatioBasedCompactionPolicy`). It's still size-tiered: it picks a contiguous run of HFiles where the largest file's size ≤ `hbase.hstore.compaction.ratio` × sum of the rest. "Exploring" just means it evaluates several candidate windows and picks the best by size/file-count tradeoff, instead of greedily taking the first qualifying run.
- **Minor vs major**: Minor compactions merge a subset of HFiles into one (size-tiered selection). Major compactions rewrite *all* HFiles for a Store into a single file and physically drop tombstones / expired TTL cells / excess versions.

### Knobs That Shape the Behavior

- `hbase.hstore.compaction.min` / `.max` — bounds on files per compaction (default 3 / 10).
- `hbase.hstore.compaction.ratio` — the size-ratio threshold (default 1.2).
- `hbase.hstore.blockingStoreFiles` — flushes block once the file count exceeds this, applying back-pressure so size-tiered selection has time to keep up.

### Alternative Policies HBase Ships

- **`FIFOCompactionPolicy`**: no rewriting; just drops whole HFiles whose cells are all TTL-expired. Useful for time-series / append-only with TTL.
- **`DateTieredCompactionPolicy`**: time-windowed size-tiering — files are bucketed by write time and only files within the same time window compact together. Closest HBase has to "tiered by recency."
- **Stripe compaction** (`StripeStoreEngine`): partitions a region's keyspace into sub-ranges ("stripes") and runs size-tiered compaction inside each stripe. This is the closest HBase gets to *leveled-ish* behavior — bounded write amplification per stripe and an L0-style staging area — but it's still size-tiered within each stripe, not true leveled compaction with non-overlapping key ranges across N levels.

### Summary

HBase is a **size-tiered LSM** by default (ExploringCompactionPolicy), with optional **date-tiered** and **stripe** variants. It does *not* implement true level-based compaction in the RocksDB/LevelDB sense — there is no L1..Ln hierarchy with non-overlapping key ranges and per-level size targets.

---

## 2. The Rationale Behind `hbase.hstore.compaction.ratio`

The ratio only governs **minor** compaction selection. Major compactions ignore it; they rewrite every HFile in the Store unconditionally.

### The Tension It Resolves

The size-tiered LSM has a fundamental tension:

- **Compact too eagerly** (merge files of very different sizes) → you rewrite a huge old file just to absorb a tiny new one. Massive **write amplification**.
- **Compact too lazily** → file count grows, read amplification explodes (every Get/Scan must consult more HFiles, more bloom filter checks, more seeks).

The ratio is the knob that says: *"only merge a small file into a larger one if the larger one isn't* too *much larger."* It keeps each compaction's output roughly proportional to its inputs, which bounds write amplification while still collapsing files frequently enough to keep read amplification in check.

### The Selection Rule

For a candidate window of HFiles ordered oldest→newest (equivalently, largest→smallest in a steady-state size-tiered store), a file `F` is eligible to be included only if:

```
size(F) ≤ ratio × sum(size of all newer/smaller files in the window)
```

With the default `hbase.hstore.compaction.ratio = 1.2`:

- A file is included only when it's no more than 1.2× the combined size of the smaller files that would be merged with it.
- This naturally **excludes the big old files** until enough small files accumulate next to them to "earn" their inclusion.

#### Worked Example

Store has HFiles (newest → oldest): `[10 MB, 12 MB, 15 MB, 200 MB, 1 GB]`, ratio = 1.2.

Walking from the smallest end and growing the window:

- Window `{10}`: trivially OK.
- Add 12: `12 ≤ 1.2 × 10`? → `12 ≤ 12` ✓
- Add 15: `15 ≤ 1.2 × (10+12) = 26.4` ✓
- Add 200: `200 ≤ 1.2 × (10+12+15) = 44.4` ✗ — stop.

Selected: `{10, 12, 15}` → merged into one ~37 MB file. The 200 MB and 1 GB files are left alone. They'll only be pulled in once the small-file tier accumulates enough mass to make the ratio test pass — which is exactly the "tiered" behavior of size-tiered compaction.

### Why This Shape of Rule

- **Bounded write amplification per compaction**: the file being absorbed is at most `ratio` times the new data merged with it, so each byte gets rewritten only a bounded number of times as it migrates up the size tiers.
- **Self-similar tiers**: with ratio ≈ 1, files cluster into roughly geometric size tiers (small, medium, large, huge). Compaction merges within a tier and promotes the result to the next tier.
- **Tunable tradeoff**:
  - **Higher ratio** (e.g. 1.5–3.0) → more aggressive merging, fewer HFiles, better read latency, **worse write amplification**. Good for read-heavy workloads.
  - **Lower ratio** (closer to 1.0) → more conservative, more HFiles linger, lower write amplification, **worse read amplification**. Good for write-heavy workloads.

### Other Guards That Interact With the Ratio

The ratio doesn't act alone. `ExploringCompactionPolicy` also enforces:

- `hbase.hstore.compaction.min` (default 3): don't bother compacting fewer than this many files.
- `hbase.hstore.compaction.max` (default 10): cap on files per compaction, to bound the size of any single compaction job.
- `hbase.hstore.compaction.min.size` (default = memstore flush size, 128 MB): files **smaller** than this are *always* eligible regardless of the ratio. This prevents many tiny flush files from getting stranded by the ratio test.
- `hbase.hstore.compaction.max.size`: files larger than this are excluded from minor compaction entirely — they only get rewritten by a major compaction.
- **Off-peak ratio** (`hbase.hstore.compaction.ratio.offpeak`, default 5.0): during a configured off-peak window, the policy uses a much larger ratio so it can absorb big files cheaply when load is low.

And the "exploring" part: rather than picking the first window that passes the ratio test, it evaluates *all* valid windows within `[min, max]` size and picks the one that maximizes files-compacted while minimizing total bytes rewritten — i.e., best read-amp reduction per unit of write-amp paid.

---

## 3. Bounded Write Amplification — Why It Holds

### What "Write Amplification" Means Here

A byte enters the LSM once (via the memstore flush that produces its first HFile). But over its lifetime, it may be **rewritten** multiple times as compactions merge its HFile with others. Write amplification = (total bytes physically written to disk) / (bytes of user data ingested). Every rewrite costs disk bandwidth and SSD wear.

The question is: **how many times will a given byte be rewritten before the store reaches steady state?**

### The Per-Compaction Guarantee

The ratio rule says: a file `F` is included in a compaction only if

```
size(F) ≤ ratio × sum(smaller files merged with it)
```

Flip that around. Let `S` = sum of the smaller files (the "new data" being merged in), and `F` = the larger file being absorbed. The rule guarantees `F ≤ ratio × S`.

The output of this compaction has size `F + S ≤ ratio × S + S = (1 + ratio) × S`.

So the **output is at most `(1 + ratio)` times the size of the new data being merged in.** With ratio = 1.2, the output is ≤ 2.2× the incoming small-file mass. Equivalently: to produce one byte of output at the next tier, we rewrote at most `(1 + ratio)` bytes — bounded, not unbounded.

This is the "bounded per compaction" half.

### Why This Bounds *Lifetime* Rewrites (The Tier-Migration Argument)

Now think about a single byte's journey. It's flushed into a small HFile (tier 0). Eventually it gets compacted with peer small files into a medium file (tier 1). Later, enough medium files accumulate that they get compacted into a large file (tier 2). And so on.

The ratio rule forces tiers to be **geometrically spaced in size**. Here's why:

- A tier-`k` file only gets pulled into a compaction once the smaller files next to it sum to at least `size(tier k) / ratio`.
- So the next tier up has size ≈ `(1 + ratio) × size(tier k)` — roughly `2.2×` larger with default settings.

This means the number of tiers needed to hold a store of total size `N` (starting from flush size `f`) is:

```
num_tiers ≈ log_{(1+ratio)}(N / f)
```

A byte gets rewritten **once per tier promotion**. So:

```
write_amp ≈ log_{(1+ratio)}(N / f)
```

#### Concrete Numbers

Flush size = 128 MB, store size = 128 GB (so `N/f` = 1000), ratio = 1.2:

- `log_{2.2}(1000) ≈ 4.3`

A byte gets rewritten ~4–5 times over its lifetime in this store. Not 100. Not unbounded. **Logarithmic in store size.**

Compare to ratio = 3.0 (more aggressive):
- `log_4(1000) ≈ 5`. Roughly the same number of tier promotions, but each compaction rewrites more old data, so per-compaction cost is higher.

Compare to ratio = 1.01 (very lazy):
- `log_{2.01}(1000) ≈ 10`. More tiers, more rewrites, but each compaction is cheap.

### The Contrast: What *Unbounded* Would Look Like

Imagine no ratio rule — just "compact any subset of files." A pathological policy could merge a 10 MB file into a 1 GB file every flush. Each merge rewrites 1 GB to absorb 10 MB → write amp = 100× **per compaction**, and the big file keeps growing, so each subsequent merge is even worse. Lifetime write amp grows linearly with the number of flushes, not logarithmically.

The ratio rule prevents this by refusing to pull the 1 GB file into a compaction until ~830 MB of smaller files have accumulated next to it. At that point, rewriting it costs ~2.2× the incoming data — still bounded.

### The Intuition in One Line

The ratio rule enforces that **each compaction rewrites old data in proportion to the new data it's absorbing**, never wildly more. Stack that bound across the geometric tier hierarchy and you get write amplification that's logarithmic in store size — the defining property of a well-tuned size-tiered LSM.
