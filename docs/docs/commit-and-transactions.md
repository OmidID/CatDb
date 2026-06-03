---
id: commit-and-transactions
title: Commit and Transactions
---

# Commit and transactions

CatDb uses explicit commits. Writes are visible to the current engine after table flush/read paths, but they are not durable until you call `engine.Commit()`.

```csharp
using (var engine = CatDb.Database.CatDb.FromFile("app.catdb"))
{
    var table = engine.OpenXTable<int, string>("events");
    table[1] = "committed";
    engine.Commit();

    table[2] = "not committed";
}

using var readEngine = CatDb.Database.CatDb.FromFile("app.catdb");
var readTable = readEngine.OpenXTable<int, string>("events");

Console.WriteLine(readTable.Exists(1)); // true
Console.WriteLine(readTable.Exists(2)); // false
```

Closing or disposing an engine does not commit pending writes.

## What Commit does

`StorageEngine.Commit()`:

1. Enters the engine synchronization lock.
2. Flushes each opened modified table into the WTree.
3. Updates table modified timestamps.
4. Calls the WTree commit path.
5. Stores dirty branches, scheme metadata, and settings into the heap.
6. Commits the heap.
7. Marks opened tables as unmodified.

## Commit modes

| Mode | Behavior |
| --- | --- |
| `CommitMode.WriteAheadLog` | Default. Writes changes to a `.wal` file and recovers or checkpoints on reopen. Crash-safe. |
| `CommitMode.InPlace` | Legacy in-place writes with atomic header pointer swap. Faster, but unsafe if a crash happens mid-commit. |

```csharp
using CatDb.Database;
using CatDb.Storage;

var options = new DatabaseOptions
{
    CommitMode = CommitMode.WriteAheadLog,
};

using var engine = CatDb.Database.CatDb.FromFile("app.catdb", options);
```

## Transaction boundaries

CatDb's transaction boundary is the engine-level commit. It is not an interactive SQL-style transaction manager with nested transactions, rollback APIs, isolation levels, or multi-version snapshots.

The practical rule is simple:

- Perform all table/index/file updates for a unit of work.
- Call `Commit()` once.
- If the process exits before commit, those uncommitted changes are lost on reopen.

## Concurrency notes

The WTree uses top-down lock ordering. Execute and commit serialize at the root lock; reads use hand-over-hand traversal from root to child. Avoid calling user code while holding branch locks in engine code.

For application code, keep commit scopes clear and avoid long-running work between table writes and `Commit()` when many threads share one engine instance.
