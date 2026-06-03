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
| `FromFile(fileName, options)` | Open or create a persistent database file. |
| `FromMemory(options)` | Tests, demos, temporary stores. |
| `FromStream(stream, options)` | Store on any seekable stream. |
| `FromHeap(heap, options)` | Custom heap implementation. |
| `FromNetwork(host, port, databaseName, userName, password)` | Remote TCP client. |
| `FromNetworkAsync(...)` | Async TCP connection setup. |
| `CreateServer(engine, port)` | Wrap an engine in the TCP storage server. |

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
