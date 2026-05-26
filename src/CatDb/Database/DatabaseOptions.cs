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
    /// Default: 4096.
    /// </summary>
    public int CacheSize { get; set; } = 4096;

    /// <summary>
    /// Default options suitable for most workloads.
    /// </summary>
    public static DatabaseOptions Default => new();
}
