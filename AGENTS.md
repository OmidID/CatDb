# CatDb — Agent Instructions

## Project Overview

CatDb is a high-performance embedded database engine written in C# targeting .NET 10.
It uses a **Waterfall Tree (WTree)** — a B-tree variant with cascading operations for write optimization.

## Build & Test

```bash
cd src
dotnet build --no-incremental          # full rebuild
dotnet test --no-build                 # 170 unit tests
```

## Stress Test

```bash
cd src/CatDb.StressTest
dotnet run -c Release -- --duration 120    # run for 120 seconds then auto-stop
dotnet run -c Release                      # run until key press or Ctrl+C
```

- `--duration <seconds>` or `-d <seconds>` — auto-stop after N seconds (recommended for CI/agent use)
- Without `--duration`, the test runs indefinitely until a key press or Ctrl+C
- Error log: `stress_errors.log` in the working directory
- Database file: `catdb_stress.db` (delete before run for a fresh start)

## Architecture — Key Files

| Path | Purpose |
|------|---------|
| `CatDb/WaterfallTree/WTree.cs` | Core WTree: Execute, FindData, Commit, EvictCache |
| `CatDb/WaterfallTree/WTree.Branch.cs` | Branch struct (node handle, cache, node loading) |
| `CatDb/WaterfallTree/WTree.Branch.Fall.cs` | Fall (cascade operations down the tree) |
| `CatDb/WaterfallTree/WTree.InternalNode.cs` | Internal node: Apply, BroadcastFall, FindBranch |
| `CatDb/WaterfallTree/WTree.InternalNode.Maintenance.cs` | Node restructuring (Split/Merge) |
| `CatDb/WaterfallTree/WTree.BranchesOptimizator.cs` | Locator→branch index lookup cache |
| `CatDb/WaterfallTree/WTree.BranchCollection.cs` | List of (FullKey, Branch) pairs |
| `CatDb/Storage/WalHeap.cs` | WAL-based crash-safe heap (ConcurrentDictionary for lock-free reads) |
| `CatDb/General/Threading/ReentrantLock.cs` | Reentrant lock helper (exists but NOT used on branches) |
| `CatDb/Database/StorageEngine.cs` | Public API entry point |

## Concurrency Model

### Locking Strategy
- **Root lock**: `lock(_rootBranch)` serializes Execute, Commit. FindData uses `Monitor.Enter(_rootBranch)`.
- **Branch locks**: `lock(this)` on each Branch (via `Monitor`). Monitor is per-thread reentrant.
- **Hand-over-hand** (lock coupling): FindData locks root → Falls cascade → then walks root→leaf locking child before releasing parent.
- **Lock ordering**: ALWAYS root → child → grandchild (top-down). Never upward. This prevents deadlocks.

### Why Monitor (not ReentrantLock) on Branches
- `Monitor` (`lock(this)`) IS already reentrant per-thread — handles same-thread re-entry natively.
- ReentrantLock was introduced for SemaphoreSlim reentrancy issues (now resolved by removing SemaphoreSlim).
- Having two lock objects (Monitor + ReentrantLock) on the same branch caused races — DO NOT reintroduce.
- The `ReentrantLock` class still exists for potential future use elsewhere.

### Critical Invariants
1. **DoFall order**: Apply cache → Maintenance → BroadcastFall (sequential within lock)
2. **Maintenance is exception-safe**: try/finally ensures Branches is ALWAYS rebuilt from helpers
3. **EvictCache is synchronous**: runs inside Commit under root lock (no background thread)
4. **BroadcastFall is sequential**: no Parallel.ForEach (prevents ThreadPool starvation)

## Known Bug Fixes (2026-05-25/26)

### 1. DoCache Background Thread Removal
- **Problem**: `Task.Run(() => DoCache(...))` polled every 1ms, contending for root lock. Under ARM64 stress, created races with hand-over-hand locking.
- **Fix**: Removed entirely. Cache eviction now runs synchronously in `Commit()` via `EvictCache()`.
- **Cache size**: Increased from 32 to 256 (reduces thrashing).

### 2. Exception-Safe Maintenance
- **Problem**: `Branches.Clear()` + exception in `helper.Run()` = permanently empty Branches → cascade of 27K+ errors.
- **Fix**: Wrapped `helper.Run()` loop in `try/finally` to always rebuild Branches from helpers.

### 3. SequentialApply Index Bug
- **Problem**: `Branches[i - 1]` crashes with `IndexOutOfRangeException` when `i = 0` (range.FirstIndex = 0 for the first base locator after tree restructuring/Split).
- **Fix**: `var branchIndex = Math.Max(0, i - 1);` — same pattern FindIndex uses.
- **File**: `WTree.InternalNode.cs` line 77

### 4. Defensive Bounds Checking
- `BranchesOptimizator.BuildRanges()`: captures count upfront, breaks on OOB
- `BroadcastFall()`: clamps firstIndex/lastIndex, returns if Branches empty
- `SequentialApply()`: early return if Branches.Count == 0
- `BranchCollection.Range()`: clamps toIndex to Count-1

### 5. WalHeap Lock-Free Reads
- **Problem**: Original `Heap` had lock contention on reads during concurrent writes.
- **Fix**: `ConcurrentDictionary<long, byte[]>` for pending writes. Read/Write/Exists are lock-free. Only Commit serializes.

### 6. FindRange Assertion Failure
- **Problem**: `BranchesOptimizator.FindRange()` crashed with `Debug.Assert(idx >= 0)` when a locator's BinarySearch insert position was 0 (locator sorts before all Branches entries). After tree Split, a node can receive a query for a locator that precedes all its branch keys, yielding `~0` from BinarySearch → `idx = ~0 - 1 = -1`.
- **Fix**: Replaced `Debug.Assert(idx >= 0)` with `if (idx < 0) idx = 0;` — route to first branch when locator precedes everything.
- **File**: `WTree.BranchesOptimizator.cs` FindRange method

### 7. DoFall `this == node.Branch` Assertion Failure
- **Problem**: `Debug.Assert(this == node.Branch)` in `DoFall` crashed when a node's `Branch` back-reference didn't match the current branch. After parent eviction (EvictCache) and reload, new Branch objects are created for the same handles. When the new branch's `Node` getter reclaims the cached node via `Tree.Retrieve`, it sets `node.Branch = newBranch`, invalidating the old branch's assertion.
- **Fix**: Replaced assertion with defensive `node.Branch = this;` — ensures ownership is correct after cache transfers. Already inside `lock(this)` so thread-safe.
- **File**: `WTree.Branch.Fall.cs` DoFall method, line 14

## Debugging Tips

- **IndexOutOfRangeException in WTree**: Usually means a Branches/optimizator inconsistency. Check if Maintenance left Branches in a bad state, or if an index from the optimizator is stale.
- **"No such handle" from Heap**: Means a handle was released but still referenced. Check WalHeap pending writes ordering.
- **ThreadPool starvation**: If throughput drops to near-zero, check for `Parallel.ForEach` or `Task.Run` inside locked sections.
- **Deadlock**: Should not occur with current top-down lock ordering. If suspected, verify no code acquires a parent lock while holding a child lock.
