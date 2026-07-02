// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Storage;

namespace CatDb.Database;

/// <summary>
/// Configuration options for a CatDb database instance.
/// </summary>
public sealed class DatabaseOptions
{
    /// <summary>
    /// How commits are persisted to disk.
    /// Default: TransactionLog (crash-safe, incremental checkpoints).
    /// </summary>
    public CommitMode CommitMode { get; set; } = CommitMode.TransactionLog;

    /// <summary>
    /// How a commit persists dirty nodes (selects an <c>ICommitStrategy</c>). <see cref="CommitDurability.Synchronous"/>
    /// (default) stores them inline under the root lock; <see cref="CommitDurability.Deferred"/> hands them to a
    /// background checkpoint to remove the commit-time lock hold (requires <see cref="CommitMode.TransactionLog"/>).
    /// </summary>
    public CommitDurability CommitDurability { get; set; } = CommitDurability.ParallelCheckpoint;

    /// <summary>
    /// (<see cref="CommitMode.TransactionLog"/> only) Run a checkpoint when this many milliseconds have
    /// elapsed since the last one. Smaller = more frequent, smaller checkpoints (shorter store stalls) but
    /// more total node I/O. Bounds crash-recovery replay time. Default: 2 s.
    /// </summary>
    public int CheckpointIntervalMs { get; set; } = 2_000;

    /// <summary>
    /// (<see cref="CommitMode.TransactionLog"/> only) Run a checkpoint when the operation log grows past
    /// this many bytes since the last one. Lower = smaller dirty-node sets per checkpoint → shorter
    /// root-lock stalls, at the cost of more frequent node flushes. Default: 8&#160;MB.
    /// </summary>
    public long CheckpointLogSizeBytes { get; set; } = 8L * 1024 * 1024;

    /// <summary>
    /// (<see cref="CommitMode.TransactionLog"/> only) Incremental (ARIES-style) checkpoint: each checkpoint
    /// flushes only the coldest <see cref="CheckpointMaxNodes"/> dirty nodes and advances the recovery
    /// boundary just past what became durable, replaying the rest from the log on open. Bounds the
    /// per-checkpoint root-lock stall. <b>Default false</b> — the full checkpoint (a consistent cut) is the
    /// proven-safe path; enable only where the shorter stall is worth the (crash-tested) recovery machinery.
    /// </summary>
    public bool IncrementalCheckpoint { get; set; }

    /// <summary>
    /// (<see cref="IncrementalCheckpoint"/>) Max dirty nodes flushed per incremental checkpoint (whole
    /// structural groups count together). Smaller = shorter stalls, more frequent checkpoints. Default: 64.
    /// </summary>
    public int CheckpointMaxNodes { get; set; } = 64;

    /// <summary>
    /// Maximum number of branches (children) per internal node.
    /// Controls tree fan-out. Higher = shallower tree, larger nodes.
    /// Default: 64.
    /// </summary>
    public int MaxBranchesPerNode { get; set; } = 64;

    /// <summary>
    /// Maximum records per leaf node before it splits.
    /// Smaller = faster splits (less stall under lock), but deeper tree.
    /// Default: 8192.
    /// </summary>
    public int MaxRecordsPerLeaf { get; set; } = 8192;

    /// <summary>
    /// Minimum records per leaf node before it's considered underflow (merge candidate).
    /// Default: 4096.
    /// </summary>
    public int MinRecordsPerLeaf { get; set; } = 4096;

    /// <summary>
    /// Maximum buffered operations in the root before triggering a Sink (cascade).
    /// Smaller = more frequent but cheaper sinks.
    /// Default: 4096.
    /// </summary>
    public int MaxOperationsInRoot { get; set; } = 4096;

    /// <summary>
    /// Maximum total buffered operations across all branches of an internal node
    /// before sinking is triggered during Maintenance.
    /// Controls intermediate node serialization size and cold-read latency.
    /// Default: 8192.
    /// </summary>
    public int MaxOperationsPerNode { get; set; } = 8192;

    /// <summary>
    /// Minimum operation threshold — sinking stops when total drops below this.
    /// Default: 4096.
    /// </summary>
    public int MinOperationsPerNode { get; set; } = 4096;

    /// <summary>
    /// Number of nodes to keep in the in-memory cache.
    /// Larger = fewer disk reads, more memory usage.
    /// Used only when <see cref="CacheSizeBytes"/> is 0.
    /// Default: 4096.
    /// </summary>
    public int CacheSize { get; set; } = 4096;

    /// <summary>
    /// Memory budget (bytes) for the in-memory node cache. Default: <b>2&#160;GB</b>.
    /// <para>
    /// This is NOT just a read cache — it gates the WTree's buffer cascade. Ops buffer in internal nodes and
    /// drain ("sink") down to their children; a sink can only drain into children that are <i>resident</i> in
    /// the cache (it skips cold/evicted children to avoid an I/O storm under the root lock). If the cache is
    /// too small the children get evicted, the sink can't drain, and the buffers pile up — a node bloats to
    /// tens of thousands of buffered ops (a &gt;1&#160;MB object). Those bloated nodes then hog the cache, so
    /// even fewer children stay resident → a self-reinforcing bloat spiral: memory climbs, GC pauses grow, and
    /// throughput collapses over minutes. The behaviour is bistable: with enough cache the tree stays in the
    /// "drained" state (small nodes, many resident, sink keeps draining) and — counter-intuitively — uses
    /// <i>less</i> total memory than a starved small cache. 512&#160;MB starved this; 2&#160;GB stays drained
    /// (validated: sink drains ~90%, cold-skips drop to zero, RSS self-regulates well under the budget).
    /// </para>
    /// The budget is a ceiling, not a reservation — the drained working set typically sits far below it.
    /// Raise it for very large/write-heavy datasets; lower it only if RAM-constrained (and watch
    /// <c>wtree.maintenance.sink.cold.skipped</c> / <c>final.ops</c> for the starvation signature).
    /// Set to 0 to fall back to the legacy fixed <see cref="CacheSize"/> node-count cache (NOT recommended:
    /// WTree nodes vary from KB to over a MB, so a node count makes the managed heap unbounded).
    /// </summary>
    public long CacheSizeBytes { get; set; } = 2L * 1024 * 1024 * 1024;

    /// <summary>
    /// Store leaf row data in <b>unmanaged (native) memory</b> via the SQL-Server/Postgres-style native
    /// slotted page (<see cref="General.Collections.NativeOrderedSet"/>) instead of the managed
    /// <see cref="General.Collections.OrderedSet{TKey,TValue}"/>. Default: <b>false</b> (the proven managed
    /// path). When enabled, keys + records live off the GC heap — no boxed key, record object, or red-black
    /// node per row — eliminating the multi-GB gen2 footprint (and the page-out freezes) at tens of millions
    /// of rows. v1 is a process-wide switch applied from the first <see cref="WaterfallTree.WTree"/> built.
    /// </summary>
    public bool UseNativeLeafStorage { get; set; } = false;

    /// <summary>
    /// Default options suitable for most workloads.
    /// </summary>
    public static DatabaseOptions Default => new();
}
