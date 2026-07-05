---
id: database-engine
title: Database Engine
---

# Database engine

The public entry point is `CatDb.Database.CatDb`, a static factory that creates `IStorageEngine` instances.

```csharp
var engine = CatDb.Database.CatDb.FromFile("app.catdb");
```

`StorageEngine` inherits from `WTree` and adds named logical structures: tables and files. Internally, each structure is described by a `Locator` and stored in the shared WTree.

## Factory methods

| Method | Use case |
| --- | --- |
| `FromNetwork(host, port, databaseName, userName, password)` | Remote TCP client — the server-first path. |
| `FromNetworkAsync(...)` | Async TCP connection setup. |
| `FromConnectionString(connectionString)` | One `Key=Value;...` string for any provider (File/Memory/Network); see below. |
| `FromConnectionStringAsync(connectionString)` | Async version — only the Network provider actually awaits. |
| `FromFile(fileName, options)` | Embedded: open or create a persistent database file. |
| `FromMemory(options)` | Embedded: tests, demos, temporary stores. |
| `FromStream(stream, options)` | Embedded: store on any seekable stream. |
| `FromHeap(heap, options)` | Embedded: custom heap implementation. |
| `CreateServer(engine, port)` | Wrap an engine in the TCP storage server. |

## Connection strings

`FromConnectionString` covers all three backends from one ADO.NET-style string — `Key=Value;Key=Value;...`, keys case-insensitive, common aliases accepted. `Provider` picks the backend explicitly (`File`/`Disk`, `Memory`/`Mem`/`InMemory`, `Network`/`Remote`/`Tcp`/`Server`); if omitted it's inferred from the other keys present (`Host` → Network, `Path` → File, else → Memory).

```csharp
using var remote = CatDb.Database.CatDb.FromConnectionString(
    "Provider=Network;Host=localhost;Port=7182;Database=default;User Id=admin;Password=secret");

using var file = CatDb.Database.CatDb.FromConnectionString(
    "Provider=File;Path=app.catdb;CommitMode=TransactionLog;CacheSizeBytes=2GB");

using var memory = CatDb.Database.CatDb.FromConnectionString("Provider=Memory;UseNativeLeafStorage=true");
```

Network keys: `Host`/`Server`/`Address`, `Port` (default `7182`), `Database`/`DatabaseName`/`Catalog`, `UserName`/`User Id`/`UID`, `Password`/`PWD`, plus `InitialPageCapacity`/`MaxPageCapacity`/`PageGrowthFactor`/`WriteBatchCapacity`/`CacheSize` for client tuning.

File/Memory keys map onto every `DatabaseOptions` property (`CommitMode`, `CommitDurability`, `CheckpointIntervalMs`, `CheckpointLogSizeBytes`, `IncrementalCheckpoint`, `CheckpointMaxNodes`, `MaxBranchesPerNode`, `MaxRecordsPerLeaf`, `MinRecordsPerLeaf`, `MaxOperationsInRoot`, `MaxOperationsPerNode`, `MinOperationsPerNode`, `CacheSize`, `CacheSizeBytes`, `UseNativeLeafStorage`), plus `Path`/`File`/`Filename` (File only), `UseCompression`/`Compression`, and `AllocationStrategy`/`Strategy`. Byte-size values accept `KB`/`MB`/`GB`/`TB` suffixes (e.g. `CacheSizeBytes=2GB`).

## StorageEngine responsibilities

- Creates and reopens named `XTABLE` and `XFILE` structures.
- Validates that reopened names match their original key and record schemas.
- Maintains table metadata such as create, modify, and access timestamps.
- Flushes modified tables before each commit.
- Persists the scheme and tree through the inherited WTree commit path.

## Database options

```csharp
using CatDb.Database;
using CatDb.Storage;

var options = new DatabaseOptions
{
    CommitMode = CommitMode.WriteAheadLog,
    MaxBranchesPerNode = 64,
    MaxRecordsPerLeaf = 8192,
    CacheSize = 4096,
};

using var engine = CatDb.Database.CatDb.FromFile("app.catdb", options);
```

| Option | Default | Meaning |
| --- | ---: | --- |
| `CommitMode` | `WriteAheadLog` | Crash-safe WAL commits by default. |
| `MaxBranchesPerNode` | `64` | Maximum children per internal WTree node before split. |
| `MaxRecordsPerLeaf` | `8192` | Maximum records per leaf before split. |
| `MinRecordsPerLeaf` | `4096` | Merge/underflow threshold. |
| `MaxOperationsInRoot` | `4096` | Buffered root operations before cascading. |
| `MaxOperationsPerNode` | `8192` | Buffered operations per internal node before sinking. |
| `MinOperationsPerNode` | `4096` | Lower threshold used while sinking. |
| `CacheSize` | `4096` | Number of WTree nodes kept in memory. |

## WTree in one pass

The WTree is a write-buffered ordered tree. Writes enter a branch cache first. When a cache grows too large, the engine performs a fall: it applies buffered operations to the current node, restructures if needed, then broadcasts operations down to child branches.

Read operations flush pending table operations before navigation, then walk the tree top-down to the target leaf. This keeps range scans ordered while letting write bursts be amortized.

## Heap and persistence

The WTree serializes branches and metadata into an `IHeap`. `FromFile` creates a `Heap` over an `OptimizedFileStream`; with the default commit mode, that heap is wrapped by `WalHeap`.

Reserved heap handles:

| Handle | Content |
| ---: | --- |
| `0` | WTree settings |
| `1` | Scheme and locators |
| `2` | Root branch cache |
| `3` | Reserved |
| `4+` | Node data |
