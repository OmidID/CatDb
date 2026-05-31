# CatDb — Agent Instructions

Single source of truth for AI agents working in this repository.
If you change architecture, core behavior, build/test flow, or public query/index API, update this file.

## Project Overview

CatDb is a high-performance embedded database engine in C# targeting .NET 10.
Its core structure is the Waterfall Tree (WTree), a write-buffered B-tree variant.

## Current Status (2026-05)

- Query API has been refactored to fluent builders.
- Secondary indexes are supported in the public API.
- Composite/object key and index field types are supported (not just primitive keys).

## Build & Validate

```bash
cd src
dotnet build --no-incremental          # full rebuild
dotnet test --no-build                 # currently 190 tests
```

Recommended smoke checks after significant changes:

```bash
cd src
dotnet run --project CatDb.GettingStarted
cd CatDb.StressTest
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
```

Guidelines:

- Prefer fluent builders in demos/tests/new code.
- Avoid explicit generic arguments when type inference can resolve them.
- Keep old compatibility helpers (`QueryTake`, `QueryBackward`, `PageAfter`, etc.) for existing callers.
- Preserve support for non-primitive key/index types.

## Concurrency Model

Lock strategy:

- Root lock: `lock(_rootBranch)` serializes execute/commit; reads enter root monitor.
- Branch lock: `lock(this)` on each branch (`Monitor`, reentrant per thread).
- Traversal: top-down hand-over-hand lock coupling (root -> child -> grandchild).
- Never acquire locks bottom-up.

Do not reintroduce dual lock systems on branch nodes. `Monitor` is the canonical branch lock.

## Critical Invariants

1. DoFall order is fixed: Apply cache -> Maintenance -> BroadcastFall.
2. Maintenance must be exception-safe and always rebuild branch collections.
3. EvictCache runs synchronously inside `Commit` under root lock.
4. BroadcastFall stays sequential (no `Parallel.ForEach`).

## Recent Reliability Fixes (2026-05)

1. Removed background `DoCache` polling thread; eviction moved into sync commit path.
2. Added exception-safe maintenance rebuild logic.
3. Fixed `SequentialApply` index underflow (`Math.Max(0, i - 1)`).
4. Added defensive bounds handling in branch/range operations.
5. `WalHeap` uses `ConcurrentDictionary<long, byte[]>` for lock-free pending reads.
6. Fixed `FindRange` lower-bound edge case (`idx < 0 => idx = 0`).
7. Replaced stale branch ownership assertion in `DoFall` with defensive reassignment.

## Debugging Shortcuts

- `IndexOutOfRangeException` in WTree: verify branch/range cache consistency and maintenance rebuilds.
- "No such handle": check WAL pending write/read ordering and released handle references.
- Throughput collapse: look for parallelism introduced in locked tree paths.
- Suspected deadlock: inspect lock acquisition order for strict top-down traversal.
