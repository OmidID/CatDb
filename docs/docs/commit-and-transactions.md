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
| `CommitMode.TransactionLog` | **Default.** A commit appends the operations to a logical operation log (`.oplog`) and fsyncs — cheap, no node serialization on the hot path. Dirty nodes are written to the heap later, at an occasional **checkpoint**. On reopen the heap (state up to the last checkpoint) is loaded and the log tail is replayed. Crash-safe, and keeps commit latency low and flat. |
| `CommitMode.WriteAheadLog` | Writes changes to a `.wal` file and recovers or checkpoints on reopen. Crash-safe. |
| `CommitMode.InPlace` | Legacy in-place writes with atomic header pointer swap. Faster, but unsafe if a crash happens mid-commit. |

```csharp
using CatDb.Database;
using CatDb.Storage;

var options = new DatabaseOptions
{
    CommitMode = CommitMode.TransactionLog,
};

using var engine = CatDb.Database.CatDb.FromFile("app.catdb", options);
```

## Checkpoints (TransactionLog)

With `TransactionLog`, a commit only hardens the operation log. Dirty tree nodes are flushed to the heap
at a **checkpoint**, which then advances the recovery boundary and truncates the log. A checkpoint fires when
either threshold is crossed:

| Option | Default | Meaning |
| --- | --- | --- |
| `CheckpointIntervalMs` | `2000` | Run a checkpoint at most this often (time-based). |
| `CheckpointLogSizeBytes` | `8 MB` | Run a checkpoint once the operation log grows past this. |
| `CommitDurability` | `ParallelCheckpoint` | `ParallelCheckpoint` serializes the dirty nodes across threads to shrink the checkpoint's lock hold (full durability on return). `Synchronous` stores them inline. |

### Incremental checkpoint (opt-in, advanced)

| Option | Default | Meaning |
| --- | --- | --- |
| `IncrementalCheckpoint` | `false` | When `true`, each checkpoint flushes only the **coldest `CheckpointMaxNodes` dirty leaves** (plus the root, dirty internal nodes, and any newly-split nodes) instead of the whole dirty set, then replays the rest from the log on reopen. Keeps steady-state checkpoint latency very low (single-digit ms). |
| `CheckpointMaxNodes` | `64` | Max dirty leaves flushed per incremental checkpoint. |

Incremental checkpoint is **off by default** — the full checkpoint is the simplest, most proven path. Enable it
only when you need consistently low checkpoint latency and have validated it for your workload. Recovery is
**idempotent in-order log replay** (no redo-skip), so committed data survives a crash regardless of which subset
of nodes a checkpoint had flushed.

## Transaction boundaries

CatDb's transaction boundary is the engine-level commit. It is not an interactive SQL-style transaction manager with nested transactions, rollback APIs, isolation levels, or multi-version snapshots.

The practical rule is simple:

- Perform all table/index/file updates for a unit of work.
- Call `Commit()` once.
- If the process exits before commit, those uncommitted changes are lost on reopen.

## Concurrency notes

The WTree uses top-down lock ordering. Execute and commit serialize at the root lock; reads use hand-over-hand traversal from root to child. Avoid calling user code while holding branch locks in engine code.

Range scans (`Forward`/`Backward`/`Scan`) page across leaf nodes and are **not snapshot-isolated**: they release the tree lock between pages, so heavy concurrent writes can change the structure mid-scan. A scan always yields keys in correct order and never loops, but under sustained concurrent modification it may stop early at a consistent ordered **prefix** rather than the full set. For a stable full snapshot, scan a quiesced table (no concurrent writers).

For application code, keep commit scopes clear and avoid long-running work between table writes and `Commit()` when many threads share one engine instance.
