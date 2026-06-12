// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Storage;

/// <summary>
/// Selects how a commit persists dirty WTree nodes — the policy is realised by an
/// <c>ICommitStrategy</c> chosen automatically from this value (strategy pattern), the same way
/// <see cref="CommitMode"/> selects the heap implementation.
/// </summary>
public enum CommitDurability
{
    /// <summary>
    /// Serialise and write every dirty node inline, under the tree's root lock, before the commit
    /// returns (current behaviour). Strongest: data is on disk when <c>Commit</c> returns. The commit
    /// holds the root lock for the whole store phase, which serialises reads and writes.
    /// </summary>
    Synchronous,

    /// <summary>
    /// Still fully durable when <c>Commit</c> returns, but the dirty nodes are serialised+written in
    /// parallel across dedicated threads instead of one-at-a-time. Cuts the commit's root-lock hold by
    /// roughly the core count (e.g. 40 ms → ~5 ms), so reads/writes queue far less — no data-loss window.
    /// </summary>
    ParallelCheckpoint,

    /// <summary>
    /// The commit hands the dirty nodes to a background checkpoint and returns immediately; the nodes are
    /// serialised+written outside the root lock. Removes the commit-time lock hold entirely, at the cost
    /// of a small delayed-durability window — committed data is recoverable from the write-ahead log after
    /// a crash, so it requires <see cref="CommitMode.WriteAheadLog"/>. Mirrors SQL Server's background
    /// checkpoint + delayed durability.
    /// </summary>
    AsyncDeferred
}
