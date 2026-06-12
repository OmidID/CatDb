// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Storage;

/// <summary>
/// Determines how commits are persisted to disk.
/// </summary>
public enum CommitMode
{
    /// <summary>
    /// In-place writes with atomic header pointer swap (legacy behavior).
    /// Fast but can corrupt the database on crash mid-commit.
    /// </summary>
    InPlace,

    /// <summary>
    /// Write-Ahead Log: all changes are first written to a separate WAL file.
    /// On commit, the WAL is finalized atomically. On next open, incomplete
    /// WAL entries are discarded (transaction never happened) and complete
    /// entries are checkpointed into the main file. Crash-safe.
    /// </summary>
    WriteAheadLog,

    /// <summary>
    /// Logical transaction log (SQL-Server model). Each commit appends the applied operations to an
    /// append-only log and fsyncs it — cheap and sequential, with NO node serialisation under the root
    /// lock (the cause of throughput decay). Dirty nodes are flushed to the heap by an occasional
    /// background checkpoint, which then truncates the log and bounds the in-memory cache. On reopen the
    /// heap (state up to the last checkpoint) is loaded and the log tail is replayed. Crash-safe.
    /// </summary>
    TransactionLog
}
