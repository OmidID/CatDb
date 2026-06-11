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
    /// Default: WriteAheadLog (crash-safe).
    /// </summary>
    public CommitMode CommitMode { get; set; } = CommitMode.WriteAheadLog;

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
    /// Memory budget (bytes) for the in-memory node cache. Because write-buffered WTree nodes vary
    /// enormously in size (tens of KB to over a MB), a fixed node <i>count</i> makes the managed heap —
    /// and therefore GC pause time — unpredictable and ever-growing, which shows up as throughput that
    /// decays the longer the process runs (a restart clears it). Bounding the cache by bytes keeps the
    /// heap flat and performance steady regardless of database size. Raise it on memory-rich servers for
    /// more cache (higher throughput); 0 falls back to <see cref="CacheSize"/>.
    /// Default: 512&#160;MB. Eviction keeps the working set bounded (no throughput decay) at any value;
    /// lower it to trade throughput for a smaller heap, raise it for more cache on memory-rich servers.
    /// </summary>
    public long CacheSizeBytes { get; set; }

    /// <summary>
    /// Default options suitable for most workloads.
    /// </summary>
    public static DatabaseOptions Default => new();
}
