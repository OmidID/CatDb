# CatDb — Agent Instructions

Single source of truth for AI agents working in this repository.
If you change architecture, core behavior, build/test flow, or public query/index API, update this file.

> [CLAUDE.md](CLAUDE.md) imports this file and tracks the active throughput-decay investigation.

## Project Overview

CatDb is a high-performance ordered key-value database in C# targeting .NET 10, deployable as a server or embedded library.
Its core structure is the Waterfall Tree (WTree).

> **Before touching any WTree code, read [docs/WaterfallTree.md](docs/WaterfallTree.md).**
> It covers the interval-model theory (from Tronkov ICEST 2016), the buffer-cascade write path,
> Fall/DoFall invariants, Maintenance split/merge, multi-locator design, locking model, and
> all critical invariants. Violating any invariant causes data corruption or throughput collapse.

## Current Status (2026-05)

- Query API has been refactored to fluent builders.
- Secondary indexes are supported in the public API.
- Composite/object key and index field types are supported (not just primitive keys).
- **CatDb v2 — breaking on-disk format (2026-07):** every persisted structure (WTree nodes, Locator,
  Scheme, WTree settings header, operation log, indexers, delta compression) used to stamp its own
  literal `VERSION` byte (a legacy STSDB-derived number, scattered 40/41 across ~20 files). Collapsed
  into ONE constant, `FormatVersion.Current` (`src/CatDb/FormatVersion.cs`) = `2`. This is a clean
  break: **no backward compatibility, no reading pre-v2 or STSDB files** — old per-type legacy-read
  branches (e.g. LeafNode/InternalNode's `VERSION_V40` fallback, WTree.Header's version 0/1 switch)
  were deleted, not kept. Bump `FormatVersion.Current` — and only that — for the next breaking change.
  Existing on-disk databases from before this change will throw on open; recreate them.

- **Internal (hidden) tables + reserved prefix (2026-07):** engine-internal tables (secondary indexes,
  server system catalog) are hidden from the public engine surface. `InternalNaming.ReservedPrefix` = `"__"`
  (`src/CatDb/Database/InternalNaming.cs`); any table whose name starts with it is internal. Index tables use
  it (`IndexDefinition.GetTableName` → `__idx_…`); system catalog uses `__system_…`. `StorageEngine` still
  tracks internal tables in `_map` for commit/lifecycle but filters them out of `GetEnumerator`/`Count`/
  `Exists`/`this[name]` and refuses public `Delete`/`Rename`. Public open methods (`Obtain`) THROW on a
  reserved-prefix name → users cannot create `__…` tables. Internal callers use
  `IStorageEngine.OpenInternalXTablePortable<TKey,TRecord>(name)` (requires the prefix; bypasses the guard).
  The remote `StorageEngineClient` throws `NotSupportedException` for it — internal tables are server-side
  only; the remote server open handler routes through the guarded public open so remote clients can't create
  `__…` tables either. Index functionality is unchanged (index tables are built via `_tree.CreateLocator`,
  never `_map`).

## Build & Validate

Repo layout: `src/` (library + server), `examples/` (runnable demos, stress test, crash writer),
`tests/` (unit tests). `CatDb.slnx` and the shared `Directory.Build.props`/`Directory.Packages.props`
live at the repo root and cover all projects.

```bash
dotnet build --no-incremental          # full rebuild, from repo root
dotnet test --no-build                 # currently 190 tests
```

Recommended smoke checks after significant changes:

```bash
dotnet run --project examples/CatDb.GettingStarted
cd examples/CatDb.StressTest
dotnet run -c Release -- --duration 120
```

Stress test notes:

- `--duration <seconds>` or `-d <seconds>`: auto-stop after N seconds.
- Without `--duration`, stress runs until key press or Ctrl+C.
- Error log: `stress_errors.log`.
- DB file: `catdb_stress.db`.

## Key Architecture Files

| Path | Purpose |
|------|---------|
| `src/CatDb/FormatVersion.cs` | Single on-disk format version for ALL persisted structures |
| `src/CatDb/WaterfallTree/WTree.cs` | Core tree operations: execute, read, commit |
| `src/CatDb/WaterfallTree/WTree.Branch.cs` | Branch lock/cache/node lifetime |
| `src/CatDb/WaterfallTree/WTree.Branch.Fall.cs` | Cache cascade (fall) |
| `src/CatDb/WaterfallTree/WTree.InternalNode.cs` | Routing and branch selection |
| `src/CatDb/WaterfallTree/WTree.InternalNode.Maintenance.cs` | Split/merge/rebalance |
| `src/CatDb/WaterfallTree/WTree.BranchesOptimizator.cs` | Locator->branch range cache |
| `src/CatDb/Storage/WalHeap.cs` | WAL heap with lock-free pending reads |
| `src/CatDb/Database/StorageEngine.cs` | Public storage engine entry point |
| `src/CatDb/Database/Indexing/*` | Secondary index implementation |
| `src/CatDb/Extensions/TableQuery.cs` | Primary key fluent query builder |
| `src/CatDb/Extensions/IndexQuery.cs` | Secondary index fluent query builder |
| `src/CatDb/Extensions/ITableQueryExtensions.cs` | Query entry points and compatibility methods |

## Query & Index API Guidance

Preferred style:

```csharp
table.Query().AtLeast(10).Take(20);
table.Query(x => x.Email).StartsWith("ada");
table.Query(x => x.City).Equals("London").Count();
table.Query(x => x.Email).AtLeast("a").AtMost("z")        // filter by index/key …
     .OrderBy(x => x.Name).OrderByDescending(x => x.Age);  // … then sort by any field/key
```

Sorting (`OrderedQuery` + `OrderingPlanner`, `src/CatDb/Extensions/`; chaining = ThenBy) picks the
cheapest plan automatically. Precedence in `GetEnumerator`: **prefix → covering composite → single-
leading drive → buffered**.

1. **Pre-ordered stream (no buffer)** — the engine already emits the requested order:
   - `OrderByKey`/`OrderByKeyDescending` on a primary-key query → `Scan`/`ScanBackward`.
   - `OrderBy`/`OrderByDescending` **on the index query's own field** → the index W-tree is read in
     order (`backward` = descending). Also `IndexQuery.Backward()`.
2. **Composite prefix scan (streaming, no residual)** — `WHERE a = v ORDER BY b[,c…]` (same
   direction) where a composite `(a, b[, c…])` index exists. Scans that index restricted to the
   `a = v` prefix → rows come out already ordered by the trailing keys, with NO residual and NO
   per-row heap fetch (only `Take` rows fetched). Implemented in the COMPILER:
   `QueryCompiler.TryComposePrefixScan` emits `IndexPrefixSeek` (→ `IQueryEngineContext.SeekPrefix` →
   `TableIndexManager.ScanPrefixKeys` → `BuildCompositeBound` sentinels) and skips the Sort. This is the
   canonical real-DB filter+sort plan. **Was a latent perf bug until 2026-06:** without it the planner did
   `Sort(Fetch(IndexEqualSeek(a)))` = fetch EVERY `a=v` record then sort = O(matches) random reads that grew
   with the table (stress scans hit 4.58 s; after the fix ~ms, SortStress ~6-7× faster). Scoped to a single
   leading equality field; multi-eq prefix + mixed-direction multi-key still fall back to fetch-then-sort.
   Covered by `CompositePrefixScanTests` (plan-shape + brute-force equality).
3. **Drive-from-sort-index (streaming, bounded memory)** — ORDER BY a *different* single-indexed
   field. Driven from that index, original query re-applied as a **residual** (engine-consistent
   comparer). Single key → pure stream; multi-key → each **equal-leading-key run** buffered + sorted.
4. **Covering composite index** — multi-key ORDER BY (a,b,…) where one index's `MemberNames` equals
   the sort keys. All-same-direction → pure stream (`TryCompositeDrive`); **mixed direction** → drive
   the composite in the lead direction and run-sort each equal-lead group by the rest.
5. **Buffered stable sort (RAM-bound)** — leading sort field has no index. Materialize + sort.

**Top-K:** whenever `Take` is present, the buffered and run-sort paths use a **bounded max-heap**
(`OrderedQuery.OrderIndices`) → O(N·log(Skip+Take)) time, O(Skip+Take) memory, instead of a full
sort or whole-group buffer. `Take`/`Skip` apply after ordering in every plan.

**Secondary-index scans STREAM via WTree seeks** (`TableIndexManager.BatchedScan` + `Sentinels`):
equality, range, prefix, forward and backward all page through the index in `ScanBatchSize` (4096)
chunks. Bounds are **exact composite keys built with min/max sentinels** (`Sentinels.TryGet` +
`BuildCompositeBound`), so the WTree's Forward/Backward stop at the bound natively — NO scanning-from-
an-end-and-skipping. Field-level inclusivity is encoded in the trailing primary-key sentinel
(`field >= from` → `(from, MIN_pk)`, `field > from` → `(from, MAX_pk)`, etc.). This also fixes
**negative/min primary keys** (the old `(field, default)` lower bound skipped `pk < default`). When a
trailing slot type has no sentinel (`string` has no max) the code falls back to a filtered scan.
Composite **prefix** scans (`FindByIndexPrefix`) support multi-column prefixes and mixed-direction
trailing (drive the lead, run-sort the rest). `GreaterThan`/`LessThan` (exclusive) and `backward`
(descending) are honored everywhere.

**Remote secondary indexes fully work** (`StorageEngineServer` index handlers + `CommandPersist`
serializers + `RemoteFieldCodec`). Index field/prefix values cross the wire as raw bytes encoded by a
`DataPersist` of the field type (the server resolves the type from the index via
`TableIndexManager.GetFieldType`/`GetPrefixType`); result `(pk, record)` lists use the connection's
Key/Record persisters. `XTableRemote.SetResult` copies results back. `IndexQuery.ConvertPair`
transforms portable `Data<Slots>` records to `TRecord` for portable-backed (remote) tables. End-to-end
covered by `RemoteIndexTests` (in-process TCP server). Find/range/prefix/exists/count + ORDER BY all
work remotely.

**Deadlock rule (critical):** writes lock main-table SyncRoot → index-table SyncRoot. An index read
must therefore NEVER hold the index scan open while doing a main-table point lookup (that inverts the
order → deadlock). `BatchedScan` closes each page's enumerator (releasing the index lock) before any
record fetch, so only one table lock is held at a time. Don't "optimize" this into a single
hold-open interleave.

Remaining gaps (small): the string-named `Query("idx")` overload has no member selector → no
residual/prefix, so it buffers. `string` (and other types lacking a max sentinel) as a trailing index
slot disables the upper-bound seek for that case (filtered-scan fallback). Mixed-direction multi-key
sorts still run-buffer the leading group rather than streaming.

**`Query(...).Count()` is fetch-free** (2026-06): `Count()` routes through `TableIndexManager.TryCountFast` →
`PlanNode.CountFast`, which counts index keys WITHOUT a per-row main-table heap point-lookup (the `Fetch`
node's `TryFetch`). It strips the order-irrelevant `Sort`, honours `Skip`/`Take` (clamp), and falls back to
full enumeration only when a residual record/key predicate needs materialized rows. Before this, `Count()` =
`LongCount()` over the record-producing plan = O(matches) random reads that grow with the table → index/sort
query throughput "fell like crazy" under load (IndexStress 161→2334 ops/s after the fix). Counts match the
native `CountByIndex` semantics (index entries stay in step with records under the table locks). Covered by
`CountFastTests` (count == enumeration across equality/range/AND/OR/ORDER BY/Take/Skip/residual/deletes).

Guidelines:

- Prefer fluent builders in demos/tests/new code.
- Avoid explicit generic arguments when type inference can resolve them.
- Keep old compatibility helpers (`QueryTake`, `QueryBackward`, `PageAfter`, etc.) for existing callers.
- Preserve support for non-primitive key/index types.

## Memory Model (bounded, Postgres-like)

CatDb's memory has two knobs, mirroring Postgres `shared_buffers` + total-RAM budget:
- **`DatabaseOptions.CacheSizeBytes`** (default 2 GB) = the node cache / "buffer pool" — the engine's working
  set. Also gates the buffer cascade (invariant #5). This is the in-engine bound.
- Node read/write buffers are **pooled** (`IHeap.TryReadPooled` / `RetainsWrittenBuffer` + `Node.Load`/`Store`)
  to cut LOH churn (RSS 3.3 GB → ~1.2 GB, fragmentation way down).

**Do NOT use a tight `System.GC.HeapHardLimit` to bound memory** — tried it (4 GB) and it BACKFIRED: with the
working set near the ceiling Server GC thrashes (collects frantically to stay under) → multi-second
`commit.hold`/`cp.evict`, throughput collapsed to ~50–100 ops. A GC cap is NOT a native buffer pool. Reverted.
The residual GC sawtooth is **per-op object allocation** (records/`Data<>`/ops, ~400 MB/s gen0) inherent to the
managed design; the only real flatteners are (a) per-op object pooling and (b) moving the synchronous
checkpoint+evict node store/I/O OFF the root lock (today a working-set ≫ cache DB stalls the world during
`EvictCache` — `cp.evict` — and the checkpoint). Those two are the deep, still-open architectural items.

## Concurrency Model

Lock strategy:

- Use only ReentrantLock to avoid any deadlock scenarios.
- Never use lock() or Monitor.Enter/Exit directly.
- **Lock order is main-table → index-table** (writes take both during index maintenance). Any code
  that reads an index and then the main table must release the index scan first — see the
  `BatchedScan` deadlock rule above. Never hold an index scan open across a main-table read.

## Critical Invariants

1. DoFall order is fixed: Apply cache -> Maintenance -> BroadcastFall.
2. Maintenance must be exception-safe and always rebuild branch collections.
3. EvictCache runs synchronously inside `Commit` under root lock.
4. BroadcastFall stays sequential (no `Parallel.ForEach`).
5. **Byte-budget cache** (`DatabaseOptions.CacheSizeBytes`, **default 2 GB**). EvictCache bounds the cache
   to `Σ ApproxByteSize × 8 ≤ CacheSizeBytes` (managed-RAM estimate) instead of node count (legacy count
   cache = setting `CacheSizeBytes`=0, NOT recommended — KB→>1 MB nodes make a node count unbounded). The
   cache **gates the buffer cascade**, not just reads: a sink drains an internal node's buffer only into
   RESIDENT children (cold ones are skipped). Too small a budget → children evicted → sink can't drain →
   nodes bloat to ~14k ops/>1 MB → bloat hogs the cache → **bistable bloat spiral** (heap multi-GB, GC
   thrash, throughput collapse over minutes). 512 MB starved this; 2 GB stays "drained" and uses LESS RAM
   (~1.1 GB) than a starved small cache. Budget is a CEILING; drained set sits below it. Eviction also only
   works if cold nodes are reachable by the tree-walk `CacheFlush` (merged nodes must be `Exclude`d).
6. **CacheFlush must descend into cold subtrees.** In `DoFall`, an expired node still runs
   `BroadcastFall` (which skips unloaded children) before storing+unloading itself — do NOT switch to
   `WalkMethod.Current` on eviction or marked descendants orphan in `_cache` and the cache never shrinks.

## Recent Reliability Fixes (2026-05)

1. Removed background `DoCache` polling thread; eviction moved into sync commit path.
2. Added exception-safe maintenance rebuild logic.
3. Fixed `SequentialApply` index underflow (`Math.Max(0, i - 1)`).
4. Added defensive bounds handling in branch/range operations.
5. `WalHeap` uses `ConcurrentDictionary<long, byte[]>` for lock-free pending reads.
6. Fixed `FindRange` lower-bound edge case (`idx < 0 => idx = 0`).
7. Replaced stale branch ownership assertion in `DoFall` with defensive reassignment.
8. **Streaming index scans + ordered index reads (2026-06):** `TableIndexManager` no longer
   materializes all matching primary keys into a `List`. `BatchedScan` pages the index (4096/hop),
   never holding the index scan open across a main-table read (deadlock-safe per the main→index lock
   order). Added descending (`backward`) and exclusive-bound index range scans; `IndexQuery.Backward()`
   and `OrderBy[Descending](indexedField)` now stream.
8b. **Cross-index drive + covering composite (2026-06):** `OrderingPlanner` + `OrderedQuery` drive
   ORDER BY a different indexed field from that field's index, re-applying the source query as a
   residual; multi-key sorts run-buffer only equal-leading-key groups, OR — when a single composite
   index covers all keys with one direction — stream the composite end-to-end (O(1) memory). Demo:
   `examples/CatDb.GettingStarted/SortDemo.cs`.
8c. **Composite prefix scan + mixed direction + Top-K (2026-06):** `WHERE a=v ORDER BY b` now scans
   the composite `(a,b)` index by the `a=v` prefix — one ordered range scan, no residual, no N+1 heap
   fetch. Mixed-direction covering composites stream the lead + run-sort groups. `Take` everywhere
   uses a bounded Top-K heap (O(Skip+Take) memory). `SortStressService` heavy reads ~40% faster.
8d. **WTree-seek bounds + multi-col/mixed prefix + Remote (2026-06):** all index range/prefix/equality
   bounds are exact composite keys built from min/max **sentinels** (`Sentinels`), so the WTree seeks
   to the bound instead of scanning-and-skipping (fixes negative-PK loss; descending no longer
   tail-scans). Prefix scans take multi-column prefixes and mixed-direction trailing. **Remote
   secondary indexes are fully implemented** (serializers, handlers, `RemoteFieldCodec`, result
   copy-back, portable-record transform) and validated by an in-process-TCP `RemoteIndexTests`.
   234/234 tests pass.
9. **Throughput-decay fix (2026-06):** all `lock()`/`Monitor` replaced with `ReentrantLock`. Locking
   on long-lived `Branch` objects (`lock(this)` / `lock(_rootBranch)`) inflated their CLR sync blocks
   and caused throughput to fall ~50% after minutes (restored only by restart). Each `Branch` now has a
   dedicated `ReentrantLock SyncRoot`; ~2x throughput, 190/190 tests pass. Use `using (x.Lock())` for
   block-scoped acquisition.
10. **Incremental checkpoint (opt-in) + Forward-scan race fix (2026-06):** `DatabaseOptions.IncrementalCheckpoint`
   (default **false**) + `CheckpointMaxNodes` (default 64). Each TransactionLog checkpoint flushes only the
   coldest N dirty leaves + root + dirty internals + every NeverStored split product (so no parent persists a
   dangling child handle), then truncates the log. Design (see `docs/WaterfallTree.md` §10A):
   `SelectIncrementalCheckpoint` MARKS `ToCheckpoint` before the Store-fall; the gate (`WTree.Branch.Fall.cs`)
   stores `ToCheckpoint || NeverStored`; **`ComputeIncrementalRecoveryLsn` computes cpLsn AFTER the fall** =
   (min `MinDirtyLsn` over still-dirty non-flushed nodes) − 1. Recovery = **idempotent in-order replay of ALL
   ops > cpLsn, NO redo-skip** (the planned PageLsn skip was UNSOUND — waterfall ops sink to leaves out of LSN
   order, so `PageLsn ≥ op` does NOT prove op applied; skipping would silently drop data — the user's prod
   data-loss fear). Per-op `Lsn` stamped in `Execute`; `Node.MinDirtyLsn`/`PageLsn`/`NeverStored` in `WTree.Node.cs`.
   Tests: `IncrementalCheckpointTests` (5) + `IncrementalCheckpointCrashTests` (out-of-process `CatDb.CrashWriter`
   killed mid-write at 4 depths). **DO NOT add `NoMaintenance` to the checkpoint Store-fall** — an earlier
   attempt did, which stopped buffer draining → 16k-op nodes → 8.6 GB DB + 4 GB heap + GC-thrash FREEZE
   (looked like a hang, 318 ops/s). Maintenance must run (it drains buffers); cpLsn-after-fall keeps it safe.
   Result: steady-state `commit.hold` **2.3 ms avg, 14–32 ms max** (full checkpoint = 2.9 s); a cold-start spike
   on the first checkpoint after a bulk-load burst is GC pressure (4 GB alloc/20 s), not the checkpoint — see GC
   metrics below. 280 tests, 0 corruption.
   **Forward-scan race (pre-existing, exposed by incremental's ~30× more frequent eviction):** `XTablePortable.Forward`
   pages leaf-by-leaf releasing the WTree root lock between pages; a concurrent split/merge or checkpoint
   eviction+reload between pages could regress the cursor → out-of-order keys → **infinite loop on an unbounded
   scan (a hang = "stress test won't stop")**. `Backward` already had a `prevFirstKey` monotonicity guard +
   same-leaf `ReferenceEquals` break; **`Forward` was missing it**. Fixed by mirroring the guard into ALL four
   forward paged loops (`Forward`/`ScanCount`/`ScanDirect`/`ScanSegments`: `prevLastKey`/`prevScanLastKey`/
   `prevSegLastKey`; stop at a correctly-ordered prefix on regression). Non-snapshot scans may now return a
   consistent PREFIX under heavy concurrent writes — same contract Backward always had.

## Debugging Shortcuts

- `IndexOutOfRangeException` in WTree: verify branch/range cache consistency and maintenance rebuilds.
- "No such handle": check WAL pending write/read ordering and released handle references.
- **Throughput decay (50k→6k over minutes, restart on same big DB → fast again):** the global root lock
  (`WTree._rootBranch.SyncRoot`). `Commit` holds it ~40 ms (max ~1 s) serialising+storing every dirty node
  (`_rootBranch.Fall` → `node.Store`); reads (`Forward`/`Backward`) hold it for their WHOLE traverse. As the
  loaded working set grows, commit hold grows → `xtable.scan.lockwait` climbs → decay. Diagnose with
  `wtree.commit.hold` / `wtree.execute.hold` / `xtable.scan.lockwait` (split from `scan.flush`). Fix
  direction: move node serialise+I/O out of the root lock; release root per-leaf during scans; finer locks.
  **Best fix shipped — `CommitMode.TransactionLog`** (SQL-Server logical-log; WTree algorithm UNCHANGED,
  additive). Commit = append ops to `Storage/OperationLog.cs` + fsync (cheap, no node store under the lock);
  dirty nodes flush at an occasional checkpoint (`WTree.CheckpointToHeap`, trigger `CheckpointDue` =
  `DatabaseOptions.CheckpointIntervalMs`/`CheckpointLogSizeBytes`) which advances the Settings-v2
  `_checkpointLsn`, then `OperationLog.Truncate`s. Reopen replays log records > checkpoint LSN via
  `WTree.RecoverFromLog` (Execute under `_replaying`). New DB writes an initial checkpoint (else reopen
  throws "Logical error"); `PersistScheme` heap-commits the scheme on first log of a new locator so replay
  resolves ids. Stress: ~21k ops/s sustained, 0 corruption, memory bounded (checkpoint evicts). Residual:
  the checkpoint stores the accumulated dirty set under the root lock → periodic multi-second `commit.hold`
  spikes; next opt = bounded-dirty-set (incremental) checkpoints.
  Partial fix — `DatabaseOptions.CommitDurability` strategy (`Storage/CommitDurability.cs`,
  `WTree.Commit.cs`, `General/Threading/ParallelExecutor.cs`): `Synchronous` (default) = old inline store;
  `ParallelCheckpoint` = parallel node store on dedicated threads (commit.hold 40→25 ms, ~2×, full durability);
  `AsyncDeferred` reserved (throws). Residual hold = the sequential `Fall` traversal → needs finer locking.
- Throughput collapse: look for parallelism introduced in locked tree paths, and for any `lock()`/
  `Monitor` on shared objects (must be `ReentrantLock` — locking `this`/Branch objects inflates sync
  blocks and degrades over time).
- Suspected deadlock: inspect lock acquisition order for strict top-down traversal.
- **"Stress test won't stop" / hang:** an unbounded forward scan looping on a regressed cursor — see the
  Forward-scan monotonicity guard (fix #10). If it returns, check `XTablePortable` forward paged loops still
  carry the `prev*Key` guard.
- **Memory/throughput grows then freezes (multi-GB DB, GB managed heap, ops/s → hundreds):** buffered ops not
  draining — a checkpoint/eviction path running with Maintenance suppressed. Maintenance drains buffers; never
  `NoMaintenance` on the checkpoint Store-fall (fix #10). Diagnose with `wtree.maintenance.sink.initial.ops`
  vs `.final.ops` (≈ equal = NOT draining) and `wtree.node.store.bytes` (multi-MB = bloated nodes).
- **Memory + throughput leak over MINUTES (heap grows unbounded, ops/s 20k→200, restart fixes, delete-heavy):**
  nodes stranded in `WTree._cache`. `MaintenanceHelper.Merge` must call `Tree.Exclude(handle)` before
  `_heap.Release(handle)` — a merged-away node freed from the heap but left in `_cache` is IMMORTAL because
  eviction is a **tree walk** (`EvictCache`→`CacheFlush`) that can't reach a node no longer in the tree.
  Diagnose with `wtree.evict.marked` vs `wtree.evict.excluded`: **marked>0 but excluded==0 ⇒ eviction is a
  no-op ⇒ nodes stranded** (the cache grows even though cold nodes are "marked"). Also: `DatabaseOptions.CacheSizeBytes`
  defaults to 512 MB (byte-budget cache) — if set to 0 it falls back to the legacy 4096-NODE count cache, and
  WTree nodes are huge/variable so a node count makes the heap multi-GB (keep the byte budget).
- **DB file grows unbounded (GBs for MBs of live data) → slow I/O decay over a long run:** the heap allocator
  never reused freed space. `Storage/Space.cs` with `FromTheCurrentBlock` + the `_activeChunkIndex` cache locks
  onto the giant file-tail free chunk (always fits → `needSearch` false → `space.alloc.scan.chunks`=0), so
  freed low-offset holes are never examined and every commit appends. Fixed: file heaps use
  `AllocationStrategy.FromTheBeginning` (`CatDb.FromFile`/`FromStream`) and `Space.Alloc` force-re-searches
  from index 0 for it → fills holes first. Bounds the file (16 GB→~120 MB). Tradeoff: first-fit scans
  ~300 holes/alloc (`scan.chunks`) + scattered writes → ~10k steady vs ~16k peak; a segregated/best-fit
  allocator (O(log n)) is the follow-up to recover throughput. Watch `heap.commit.stream.after.bytes` (file
  size) vs `gauge.heap.used.count`×avg-node-size (live data) — a big gap = dead space not being reclaimed.
- **Throughput oscillates/decays + memory climbs (cache-starved buffer cascade):** the sink can't drain
  because children aren't resident. Diagnose with `wtree.maintenance.sink.cold.skipped` (>0 = skipping cold
  children) and `sink.initial.ops ≈ sink.final.ops` (≈ equal = NOT draining → nodes bloat to ~14k ops/>1 MB).
  Fix = raise `CacheSizeBytes` until `cold.skipped`→0 and `final.ops` ≪ `initial.ops` (default 2 GB does this);
  it's bistable so a too-small cache spirals into bloat (heap multi-GB). The drained state uses LESS RAM.
- **Leak hunting — the gauge tool:** `PerformanceCheck.RegisterGauge(name, () => structure.Count)` samples a
  structure's current SIZE once per 20 s window (off the hot path; ALWAYS inside `#if PERFORMANCE_CHECK`).
  Registered: `gauge.wtree.cache.count`/`cache.bytes.mb`/`cachesizebytes.mb`/`oplog.size.mb` (WTree ctor),
  `gauge.heap.used.count`/`reserved.count` (Heap ctor). Watch which climbs while throughput falls = the leak.
  Plus `rt.proc.workingset.mb` (RSS, catches native growth) and `rt.gc.*` per-window deltas.
- **commit.hold spike — is it GC or checkpoint I/O?** Per-window GC deltas now in `PerformanceCheck.FlushUnsafe`:
  `rt.gc.pause.ms.window`, `rt.gc.alloc.mb.window`, `rt.gc.gen0/1/2.window` (via `GC.GetTotalPauseDuration`/
  `GetTotalAllocatedBytes`/`CollectionCount`). Compare a window's `wtree.commit.hold` max against its
  `rt.gc.pause.ms` + alloc — a big hold with high GC pause/alloc = GC, not checkpoint (`cp.fallstore`/
  `cp.heapcommit`/`node.store.us` localise the checkpoint side). Bulk-load cold-start spikes are GC pressure
  (alloc storm), not the checkpoint.
