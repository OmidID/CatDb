# WaterfallTree — Theory and Implementation Reference

> Primary source: Tronkov, I. "NoTree", ICEST 2016 (ICEST2016_55.pdf) — positions WTree in the
> indexing-structure landscape via a unified **interval model**. Original WTree papers: "WaterfallTree
> — External indexing data structure" (IEEE AQTR 2014); "Mathematics behind WaterfallTree" (UNITECH
> 2014); "WaterfallTree implementation details" (UNITECH 2014).

---

## 1. The Interval Model (from the paper)

The ICEST 2016 paper introduces a unified way to compare all indexing families using *intervals of
keys* rather than individual key comparisons. The central insight:

> "The keys from each leaf of the B-tree define an interval which is [minimal key from leaf,
> maximal key from leaf]. Such a set of intervals is called a **level of intervals**."

Every indexing structure can be described by its *intervals configuration* — how many levels exist
and what constraints govern their overlap. The paper's taxonomy (Fig. 4):

### A) B+-tree — one level of intervals

All data lives in leaves. Leaves form one level of non-overlapping intervals. Internal node keys
are routing metadata only (excluded from the model). Random writes cause different leaves to be
accessed per key → one I/O per record when data doesn't fit in RAM.

### B) Buffer Tree / FractalIndex Tree / WaterfallTree — multiple levels with subordination

To amortize the random-I/O cost, **message buffers** are added to internal nodes. Each internal
node's buffer keys define their own interval. The constraint:

> "Every interval can intersect only with the intervals of its subtree. Every level can have at
> most `(number of branches)^(level-1)` intervals."

This means WaterfallTree has **as many interval levels as its tree height**, with strict
subordination. Writes accumulate in buffers near the root and flow downward in batches (the
"waterfall" motion). The bottleneck: if writes arrive faster than the root buffer empties,
the root becomes a throughput ceiling for the whole tree.

### C) LSM-tree — relaxed subordination, flat levels

Removes the subordination requirement. Has one in-memory AVL-tree level plus multiple B-tree
levels on disk. Each deeper level has more intervals than the previous. Merging always happens
between two adjacent levels.

### D) NoTree — no relations between intervals at all

Writes go directly to append-only files. No root, no subordination, no tree shape. The only
limit is disk write throughput. Indexing happens in cycles of **k-partitioning + multiway merge**
across any subset of intervals (not just adjacent levels).

The paper's key insight about WTree vs the others:

> "The intervals do not change their levels and the merging of the data is always between two
> adjacent levels. [NoTree allows] swapping of intervals between levels and merging multiple
> levels simultaneously."

---

## 2. Why WaterfallTree Is Faster Than B+-tree

The paper quantifies (for B+-tree with random keys): "its speed drops to under 500 rec/sec
during the tests. After 5 days it cannot insert even half of the records."

The reason, stated in the paper: random keys end up in different intervals (leaves) — every
single record requires a separate random I/O. The buffer-tree fix moves records in groups.

WTree's group-write guarantee: writes accumulate in internal node caches. When a buffer exceeds
its threshold, the whole batch falls one level together — one I/O moves many records. The
amortized I/O cost is `O(log_b(N) / B)` per operation where `B` is the block/buffer size.

---

## 3. Tree Structure

```
                     ┌─────────────────────────────────┐
                     │  Root Branch                    │  ← BranchCache (RAM buffer)
                     │  InternalNode [H0 | H1 | H2 …]  │  ← NodeHandle → IHeap (disk)
                     └──────┬───────────┬──────────────┘
                            │           │
               ┌────────────┘           └──────────────┐
               │  InternalNode                         │  InternalNode
               │  BranchCache                          │  BranchCache
               └──────┬──────┬──────┐                  └──────┬──────┐
                      │      │      │                         │      │
                   Leaf    Leaf   Leaf                     Leaf    Leaf
```

Each **Branch** is the logical unit: it holds a `BranchCache` (RAM buffer of pending operations)
and a `NodeHandle` (disk address of the actual node). The Branch is the buffer; the Node is the
data. This separation is what makes the waterfall motion possible — the buffer fills while the
node stays on disk.

### Key classes

| Class | Role |
|-------|------|
| `WTree` | Public API; owns root branch, IHeap, Scheme, in-process node cache |
| `Branch` | Logical tree node: `BranchCache` + `NodeHandle` + `ReentrantLock SyncRoot` |
| `BranchCache` | Operation buffer: `Dictionary<Locator, IOperationCollection>` |
| `InternalNode` | Routing: sorted `BranchCollection` + `BranchesOptimizator` range cache |
| `LeafNode` | Data: `Dictionary<Locator, IOrderedSet<IData,IData>>` |
| `Locator` | Identifies one logical table/index (name, key/record types, comparers, serializers) |
| `IHeap` / `WalHeap` | WAL-based block store; nodes addressed by `long` handles |

---

## 4. Multi-Locator Design

A single `WTree` hosts **multiple logical tables and secondary indexes**. Each is identified by
a `Locator` (64-bit ID + full type metadata). All locators share the same branch/node hierarchy.

- A `BranchCache` is `Dictionary<Locator, IOperationCollection>` — operations for different tables
  live in the same node buffer.
- A `LeafNode` is `Dictionary<Locator, IOrderedSet>` — records for different tables live in the
  same leaf.
- `BranchesOptimizator` caches per-locator `Range { FirstIndex, LastIndex }` in the branch list
  so routing is O(1) for the common case.

---

## 5. Write Path: Buffer → Sink → Fall

### 5.1 Buffer (Execute)

```csharp
// WTree.cs:Execute
_rootBranch.ApplyToCache(operations);   // append to root BranchCache
if (_rootBranch.Cache.OperationCount > _internalNodeMaxOperationsInRoot)
    Sink();                              // trigger waterfall cascade
```

No disk I/O. Operations are logged in the root's RAM buffer.

### 5.2 Sink (root overflow)

```csharp
// WTree.cs:Sink
_rootBranch.MaintenanceRoot(token);   // grow tree if root node itself overflowed
_rootBranch.Fall(_depth + 1, token,   // push root buffer one level down
    new Params(WalkMethod.Current, WalkAction.None, null, sink: true));
```

`MaintenanceRoot`: if the root's actual node is in Overflow state (too many branches), it wraps
the current root in a new InternalNode parent, increasing `_depth` by 1.

### 5.3 Fall (the waterfall cascade)

`DoFall` on a branch — order is a **hard invariant**:

```
1. Apply cache:     flush BranchCache into Node.Apply()
                    → InternalNode routes ops to child caches
                    → LeafNode applies ops to IOrderedSet
2. Maintenance:     split/merge overflowed/underflowed children
3. BroadcastFall:   recurse into child branches (controlled by WalkMethod)
```

`WalkMethod` controls the recursion:

| Value | Behavior |
|-------|----------|
| `Current` | Apply to this node only, no children |
| `Cascade` | Recurse into all matching children |
| `CascadeFirst` | Recurse into first matching child only |
| `CascadeLast` | Recurse into last matching child only |
| `CascadeButOnlyLoaded` | Recurse only into children already in RAM |

`WalkAction` flags: `Store` (serialize node to disk), `Unload` (remove from RAM cache),
`CacheFlush` (eviction walk), `NoMaintenance` (block recursive Maintenance during child sink).

---

## 6. InternalNode: Routing

`InternalNode` maintains a sorted `BranchCollection` of `(FullKey → Branch)` pairs.
`FullKey = (Locator, IData key)`.

**SequentialApply** (hot path for bulk inserts / index maintenance): when all operations are
monotone (keys increasing) and point-scoped, routes the entire batch to one child in O(1)
using the `BranchesOptimizator` range cache.

**General Apply**: for mixed/range ops, binary-searches the optimizator to find
`[firstIndex..lastIndex]`, then calls `branch.ApplyToCache` for each covered child.

---

## 7. Maintenance: Split and Merge

After `Apply`, if any child reported `Overflow` or `Underflow`, `Maintenance(level, token)` runs:

1. Snapshot all branches into `MaintenanceHelper[]`, clear `Branches`.
2. Run helpers **right-to-left** (right neighbor resolved first — no wait needed).
3. Each helper:
   - Falls its branch with `WalkAction.None | NoMaintenance` (one level, no recursion).
   - `Overflow` → `Split()` → inserts right sibling into the helper's local list.
   - `Underflow` → merges with right neighbor (which is already resolved).
4. Rebuild `Branches` from all helpers in `finally` (exception-safe).
5. `RebuildOptimizator()`.

**Sink phase** (follows split/merge): if total child `Cache.OperationCount > _internalNodeMaxOperations`,
sorts children descending by queue depth and falls each hottest child with
`WalkMethod.Current, Sink=true` until total drops to `_internalNodeMinOperations`.
Cold (unloaded) children skip unless total > 3× max.

---

## 8. LeafNode: Data Storage

`LeafNode._container = Dictionary<Locator, IOrderedSet<IData,IData>>` — one sorted set per
logical table. `Apply` dispatches to `locator.Apply.Leaf(ops, set)`.

**Split**: median-split at `RecordCount / 2`. Single-locator case splits the sorted set directly.
Multi-locator case distributes whole locator sets to left/right, splitting the boundary locator
if needed.

**Merge**: iterates right node, merges each set. Uses `EnterWrite` on target and `EnterRead`
on source to prevent concurrent corruption during concurrent `Fall→Apply`.

**Overflow**: `RecordCount > _leafNodeMaxRecords` (default 8 192).
**Underflow**: `RecordCount < _leafNodeMinRecords` (default 4 096) and not root.

---

## 9. Read Path (FindData)

Reads must see a consistent view including any buffered-but-not-yet-flushed operations.

```
FindData(originalLocator, locator, key, direction)
├─ Acquire root lock
├─ Fall(Sink=true, WalkMethod=Cascade/CascadeFirst/CascadeLast)
│    ← flush buffered ops for this locator path down to leaf
└─ Walk InternalNode chain:
     FindBranch(locator, key, direction) → child
     acquire child.SyncRoot, release parent.SyncRoot   ← lock hand-over (one at a time)
     if Backward && child cache still has locator → Fall(WalkMethod.Current)
   reach LeafNode:
     leaf.FindData(locator, direction) → IOrderedSet
```

`nearFullKey` / `hasNearFullKey` are out-params that give the caller the next branch boundary
— used by range scanners to page across leaf nodes without re-entering from the root.

**Paged-scan monotonicity guard (critical).** `FindData` returns ONE leaf and releases the lock; the
enumerator (`XTablePortable.Forward`/`Backward`/`ScanCount`/`ScanDirect`/`ScanSegments`) calls it again per
leaf, releasing the WTree root lock **between pages**. So a scan is not isolated: a concurrent split/merge or a
checkpoint eviction+reload between pages can make the next descent land on a leaf whose first key ≤ the last
key already yielded — the cursor jumps **backward**. Unchecked, that yields out-of-order keys and, on an
unbounded scan, **loops forever** (re-reading the same range = a hang). Every forward loop therefore tracks the
last-yielded key and, if the next page regresses (`first ≤ prev`, or the same leaf is returned), **stops at a
correctly-ordered prefix**. Backward loops do the symmetric check (`prevFirstKey`). This is not snapshot
isolation — under heavy concurrent writes a scan may return a prefix — but it is always ordered and terminates.

---

## 10. Commit (Durability)

```
Commit()
├─ Fall(CascadeButOnlyLoaded, Store)   — serialize all dirty loaded nodes to IHeap
├─ Write HANDLE_SETTINGS               — branching params (survives restart)
├─ Write HANDLE_SCHEME                 — Locator registry
├─ Write HANDLE_ROOT                   — root BranchCache (pending ops survive restart)
├─ EvictCache()                        — LRU: find coldest nodes, Fall(CacheFlush|Unload)
└─ heap.Commit()                       — WAL flush, atomic page swap
```

**Root cache persistence**: the root `BranchCache` is written to `HANDLE_ROOT` at every commit.
On next open, pending operations that never made it to leaves are restored — no data loss.

**LRU eviction**: O(N) partial-select to find `evictCount` coldest nodes by `TouchId` without
a full sort. Root is never evicted. After marking expired nodes, a `CacheFlush` walk stores and
unloads each one.

The flow above is `CommitMode.InPlace`/`WriteAheadLog` (and the TransactionLog *checkpoint*). The default
`CommitMode.TransactionLog` decouples commit from node storage — see §10A.

---

## 10A. TransactionLog Commit + Checkpoint + Recovery (default)

`CommitMode.TransactionLog` (default) makes a commit cheap and keeps latency flat: it does **not** serialize
nodes under the root lock. Instead:

```
Commit()  (TransactionLog)
├─ append this commit's ops to OperationLog (Storage/OperationLog.cs) + fsync   ← the whole durable cost
└─ if CheckpointDue() → CheckpointToHeap()      ← occasional, not every commit
```

`CheckpointDue` = `CheckpointIntervalMs` elapsed **or** log `SizeBytes ≥ CheckpointLogSizeBytes`.

### Checkpoint

A checkpoint is the §10 flow (Fall→Store, write SETTINGS/SCHEME/ROOT, EvictCache, heap.Commit) plus: it
advances the persisted recovery boundary `_checkpointLsn` (Settings v2) and then `OperationLog.Truncate`s the
log prefix that is now durable in the heap. **Maintenance runs during the checkpoint Store-fall** — it drains
buffered ops down the tree; skipping it lets buffers pile into single multi-MB nodes (heap + managed-heap
bloat → GC-thrash freeze), so it must stay on.

### Incremental checkpoint (opt-in, `IncrementalCheckpoint`, default off)

A full checkpoint stores every dirty node. Incremental stores only a **bounded** subset per pass:

```
SelectIncrementalCheckpoint()   (before the Store-fall — MARK only)
  mark ToCheckpoint: root + every dirty internal + coldest CheckpointMaxNodes dirty leaves (by MinDirtyLsn)
Store-fall (Maintenance ON)
  gate stores a node iff  ToCheckpoint || NeverStored        (NeverStored = a split product with no image yet,
                                                              incl. ones this fall's Maintenance just created)
ComputeIncrementalRecoveryLsn()  (after the Store-fall — tree has settled)
  cpLsn = (min MinDirtyLsn over still-dirty, non-flushed nodes) − 1 ; clear the ToCheckpoint marks
```

Per-node LSN tracking (`Node.MinDirtyLsn` = oldest unflushed op; `PageLsn` = max applied, persisted but
advisory; `NeverStored`) lives in `WTree.Node.cs`; each op carries a transient `Lsn` stamped in `Execute`.
Computing `cpLsn` **after** the fall is what makes it safe: Maintenance may move an op to a different node
during the fall, and the post-fall `MinDirtyLsn` reflects that, so no op ≤ `cpLsn` can be left non-durable.

### Recovery (reopen)

```
RecoverFromLog():  heap loaded (state ≤ _checkpointLsn) → replay every OperationLog record with lsn > _checkpointLsn,
                   in LSN order, via Execute under _replaying
```

Replay is **idempotent in-order** — there is deliberately **no redo-skip**. A waterfall sinks ops to leaves
*out of LSN order*, so a node's `PageLsn ≥ op` does **not** prove that op was applied; skipping ops on that
basis would silently drop data. Replaying the whole `> cpLsn` suffix in order reproduces the committed state
regardless of which subset of nodes the last checkpoint had flushed (Replace = last-writer, Delete = set-state).
A new database writes one initial checkpoint on create (so a no-write reopen finds heap structure); a split
product is always given an on-disk image in the same checkpoint, so no persisted parent ever references a
dangling child handle.

> Status: incremental steady-state checkpoint hold is single-digit–low-tens of ms (vs multi-second for a full
> checkpoint of a large dirty set). A cold-start spike can occur on the first checkpoint after a bulk-load
> burst — it is GC pressure from the workload's allocation rate, not the checkpoint logic.

---

## 11. Handles Layout (IHeap)

```
Handle 0 = HANDLE_SETTINGS   (tree params)
Handle 1 = HANDLE_SCHEME     (Locator registry)
Handle 2 = HANDLE_ROOT       (root BranchCache)
Handle 3 = HANDLE_RESERVED
Handle 4+ = node pages        (one per InternalNode or LeafNode)
```

---

## 12. Locking Model

| Lock | Scope | Rule |
|------|-------|------|
| `Branch.SyncRoot` (ReentrantLock) | per-Branch | Serializes Fall, ApplyToCache |
| `IOrderedSet.Lock` (reader-writer) | per ordered set | Protects leaf data reads/writes |
| Root SyncRoot | `_rootBranch.SyncRoot` | Execute, FindData, Commit all start here |

**Lock order: root → child (strict top-down). Never invert.**
Index reads that need a main-table lookup must release the index scan before acquiring the
main-table lock (`BatchedScan` design).

**ReentrantLock everywhere — never `lock()` / `Monitor.Enter/Exit` on Branch objects.**
Locking long-lived objects inflates their CLR sync blocks; overhead accumulates over minutes
and halves throughput (restored by restart). `ReentrantLock` uses a private `Monitor` on a
separate object; Branch sync blocks stay flat.

---

## 13. Key Configuration

| Field | Default | Meaning |
|-------|---------|---------|
| `_internalNodeMaxBranches` | 64 | Branching factor; overflow triggers split |
| `_internalNodeMinBranches` | 2 | Underflow triggers merge |
| `_internalNodeMaxOperationsInRoot` | 4 096 | Root cache → Sink threshold |
| `_internalNodeMaxOperations` | 8 192 | Child sink trigger |
| `_internalNodeMinOperations` | 4 096 | Sink stops here |
| `_leafNodeMaxRecords` | 8 192 | Leaf split threshold |
| `_leafNodeMinRecords` | 4 096 | Leaf merge threshold |
| `_cacheSize` | 4 096 nodes | In-process LRU node cache size (used when `CacheSizeBytes`=0) |

Durability / checkpoint options (`DatabaseOptions`):

| Field | Default | Meaning |
|-------|---------|---------|
| `CommitMode` | `TransactionLog` | `TransactionLog` (log-append commit + checkpoint), `WriteAheadLog`, `InPlace` |
| `CommitDurability` | `ParallelCheckpoint` | `ParallelCheckpoint` (parallel node store), `Synchronous` (inline) |
| `CheckpointIntervalMs` | `2000` | Time-based checkpoint trigger |
| `CheckpointLogSizeBytes` | `8 MB` | Log-size checkpoint trigger |
| `IncrementalCheckpoint` | `false` | Flush only the coldest `CheckpointMaxNodes` dirty leaves per checkpoint |
| `CheckpointMaxNodes` | `64` | Bounded dirty-leaf flush count per incremental checkpoint |
| `CacheSizeBytes` | `0` | Byte-budget node cache (0 = count-based `_cacheSize`) |

Depth formula (`GetMinimumlWTreeDepth`):
```
depth = ⌈ log_b( ((N - r)(b-1) + b·I) / (l·(b-1) + I) ) + 1 ⌉
```
`b` = max branches, `r` = root op limit, `I` = max ops/internal node, `l` = max leaf records.

---

## 14. Critical Invariants

1. `DoFall` order: **Apply cache → Maintenance → BroadcastFall**. Never reorder.
2. Maintenance rebuild is exception-safe: `Branches` assembled from helpers in `finally`.
3. `EvictCache` runs inside `Commit` under root lock — synchronous, no background thread.
4. `BroadcastFall` is sequential — no `Parallel.ForEach` (ThreadPool starvation under root lock).
5. `NoMaintenance` prevents recursive Maintenance during child sink (one level at a time).
6. `CacheFlush` walk skips Maintenance — no disk I/O cascades during eviction.
7. Lock on Branch = `ReentrantLock`; never `lock(this)` or any Monitor on Branch/tree objects.
8. `MaintenanceHelper` runs right-to-left so right neighbor is fully resolved before merge check.
9. **Maintenance MUST run during the checkpoint Store-fall** (drains buffered ops down the tree). Skipping it
   bloats nodes → multi-GB DB + GC-thrash freeze.
10. **Incremental `cpLsn` is computed AFTER the Store-fall**, not before — the post-fall `MinDirtyLsn` reflects
    any op Maintenance moved, so no op ≤ `cpLsn` is left non-durable.
11. **Recovery replay is idempotent in-order with NO redo-skip** — a waterfall sinks ops out of LSN order, so
    `PageLsn ≥ op` does not prove the op was applied; skipping would silently drop data.
12. **Paged forward scans carry a monotonicity guard** (last-yielded key; stop on regression) — without it a
    concurrent split/eviction between pages makes an unbounded scan loop forever.
