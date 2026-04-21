# CatDb — Agent Reference

This file is the single source of truth for any AI agent working on this codebase.
Read it fully before making any changes.

---

## 1. Project Overview

CatDb is an **embedded, ordered key-value store** written in C# / .NET 10.  
It is a re-implementation of STSdb4.  Its central data structure is the **Waterfall Tree (WTree)** —
a write-optimised B-tree variant where writes are buffered high in the tree and lazily propagated
("fallen") to leaf nodes in the background, dramatically reducing write amplification.

## Important notes
- WHEN agent refactor or modify something important, update the AGENT.md file if it needs to be reflected in the architecture overview or build/test instructions.
- Run all tests and at least the GettingStarted for verifying the changes.

### Solution Layout

```
src/
  CatDb/                  — core library (the engine, data model, storage, remote)
  CatDb.Tests/            — xUnit tests (133 tests, all must pass)
  CatDb.StressTest/       — long-running concurrent stress test
  CatDb.GettingStarted/   — usage demo (1 M record insert + read)
  CatDb.Server/           — Windows-Service TCP server (WinForms GUI)
CHANGELOG.md
README.md
```

### Build & Test Commands

```bash
cd src
dotnet build CatDb/CatDb.csproj        # build library only
dotnet test  CatDb.Tests               # 133 tests — must be green before committing
dotnet run   --project CatDb.GettingStarted   # quick smoke test
dotnet run   --project CatDb.StressTest       # concurrent stress test (run for ~60 s)
```

**CI contract:** 0 build errors, 0 build warnings, 133/133 tests green.

---

## 2. Core Layers (bottom → top)

```
┌─────────────────────────────────────────────────┐
│  User API  ITable<TKey,TRecord>                  │  Database/
│  XTable<K,R>  XTablePortable<K,R>               │
├─────────────────────────────────────────────────┤
│  Storage Engine  StorageEngine : WTree           │  Database/StorageEngine.cs
│  Locator / Scheme  (descriptor per table)        │  WaterfallTree/
├─────────────────────────────────────────────────┤
│  WTree  (Waterfall Tree)                         │  WaterfallTree/WTree*.cs
│  InternalNode  LeafNode  Branch  BranchCache     │
├─────────────────────────────────────────────────┤
│  Heap  (block storage, atomic commit)            │  Storage/Heap.cs
│  Space / Pointer / Ptr  (free-space manager)     │  Storage/
├─────────────────────────────────────────────────┤
│  Stream  (OptimizedFileStream / MemoryStream)    │  General/IO/
└─────────────────────────────────────────────────┘

Cross-cutting
  Data/          — DataType, Data<T>, IData, persist/compare/transform helpers
  Remote/        — TCP client/server wrapping the IStorageEngine interface
  General/       — collections, compression, communication, extensions
```

---

## 3. The Waterfall Tree (WTree)

### 3.1 Concept

The WTree is a **write-buffered B-tree**.  Instead of routing every write
immediately to the correct leaf, writes are collected in a *buffer (cache)* at
each internal node and flushed downward only when the buffer is full.
This turns many random writes into sequential bulk writes — the key performance
gain over a standard B-tree.

```
Root (InternalNode)
  Cache: [op1, op2, …  up to _internalNodeMaxOperationsInRoot = 8K ops]
  ├── Branch A
  │     Cache: [op…  up to 64K ops]
  │     └── Branch A1 (LeafNode)
  │           records: ordered set of IData → IData
  └── Branch B
        Cache: …
        └── Branch B1 (LeafNode)
```

When a node's cache overflows the **Fall** operation propagates ("falls") the
buffered operations one level down, either recursively or via a background task.

### 3.2 Key Classes

| Class | File | Role |
|-------|------|------|
| `WTree` | `WTree.cs` | Entry point: `Execute()`, `FindData()`, `Commit()`, `Close()` |
| `WTree` (partial) | `WTree.*.cs` | Split across 10 partial-class files |
| `Branch` | `WTree.Branch.cs` | One node slot in the tree; holds `NodeHandle`, `Cache`, lazy-loaded `Node` |
| `Branch` (partial) | `WTree.Branch.Fall.cs` | Async fall logic: flushes cache to child nodes |
| `BranchCache` | `WTree.BranchCache.cs` | Per-branch buffer of pending `IOperationCollection` keyed by `Locator` |
| `BranchCollection` | `WTree.BranchCollection.cs` | Sorted list of `(FullKey → Branch)` inside an `InternalNode` |
| `InternalNode` | `WTree.InternalNode.cs` | Routes incoming operations to the correct child branch |
| `InternalNode` (partial) | `WTree.InternalNode.Maintenance.cs` | Split / merge / rebalance logic |
| `LeafNode` | `WTree.LeafNode.cs` | Stores actual records in per-`Locator` `IOrderedSet<IData,IData>` dictionaries |
| `Node` | `WTree.Node.cs` | Abstract base: `Apply()`, `Split()`, `Merge()`, `Store()`, `Load()` |
| `FullKey` | `WTree.FullKey.cs` | `(Locator, IData key)` — composite key used for tree navigation |
| `Settings` | `WTree.Header.cs` | Serialises/deserialises tree header to `HANDLE_SETTINGS` block |
| `BranchesOptimizator` | `WTree.BranchesOptimizator.cs` | Caches last-used branch range per locator for sequential-write fast path |

### 3.3 Write Path

```
client code
  table[key] = value
    → XTablePortable.Execute(ReplaceOperation)
      → WTree.Execute(IOperationCollection)
        → lock(rootBranch)
          → rootBranch.Cache.Apply(operation)  // buffered!
          → if cache.OperationCount > 8K → Sink()
            → rootBranch.Fall(depth+1, …)
              → DoFall():  apply cache to node, then push to child branches
                           (async task per branch)
```

### 3.4 Read Path

```
client code
  table.TryGet(key)
    → XTablePortable.TryGet(key)
      → Flush()  // force all buffered ops to tree first
      → WTree.FindData(locator, key, Direction.Forward)
        → lock(rootBranch)
          → rootBranch.Fall(…) for Cascade walk
          → traverse InternalNode.Branches until LeafNode
          → lock(leafData)
            → leafData.TryGetValue(key, out record)
```

### 3.5 Commit / Persistence

`WTree.Commit()` serialises all dirty nodes to the `IHeap`, then calls
`Heap.Commit()` which atomically persists two copies of the header (double-write
protocol) to `HANDLE_SETTINGS`, `HANDLE_SCHEME`, and `HANDLE_ROOT` blocks.

```
StorageEngine.Commit()
  → for each table: Flush pending ops → set ModifiedTime
  → WTree.Commit()
    → for each dirty Branch: node.Store() → heap.Write(handle, bytes)
    → Scheme.Serialize() → heap.Write(HANDLE_SCHEME, …)
    → Settings.Serialize() → heap.Write(HANDLE_SETTINGS, …)
    → heap.Commit()  ← single atomic flush
```

### 3.6 Tunable Parameters

| Field | Default | Meaning |
|-------|---------|--------|
| `_internalNodeMinBranches` | 2 | minimum children per internal node |
| `_internalNodeMaxBranches` | 5 | maximum children (triggers split) |
| `_internalNodeMaxOperationsInRoot` | 8 192 | root cache capacity before Sink() |
| `_internalNodeMinOperations` | 32 768 | lower bound of non-root internal cache |
| `_internalNodeMaxOperations` | 65 536 | upper bound (triggers flush) |
| `_leafNodeMinRecords` | 8 192 | leaf split threshold (lower) |
| `_leafNodeMaxRecords` | 65 536 | leaf split threshold (upper) |
| `CacheSize` (StorageEngine) | — | number of nodes kept in `BranchCache` LRU |

---

## 4. Storage Engine

### 4.1 `StorageEngine` : `WTree`

File: `Database/StorageEngine.cs`

- Inherits `WTree`; adds the concept of **named tables** managed via a
  `Dictionary<string, Item1>` (`_map`).
- Each named table is a `Locator` (describes the key/record types) paired with
  an `XTablePortable` (the `ITable<IData,IData>` implementation).
- Thread-safe via `_syncRoot`.

```csharp
// entry points
ITable<IData,IData>    OpenXTablePortable(name, keyDataType, recordDataType)
ITable<TKey,TRecord>   OpenXTablePortable<TKey,TRecord>(name, …)
ITable<TKey,TRecord>   OpenXTable<TKey,TRecord>(name)   // typed, uses XTable<K,R>
XFile                  OpenXFile(name)                  // random-access byte stream
void                   Commit()
void                   Delete(name) / Rename(name, newName) / Exists(name)
```

### 4.2 Table Variants

| Class | Description |
|-------|-------------|
| `XTablePortable` | `ITable<IData,IData>` — the actual WTree table. All writes/reads go here. |
| `XTable<TKey,TRecord>` | Wraps `XTablePortable`; boxes/unboxes keys and records as `Data<T>`. |
| `XTablePortable<TKey,TRecord>` | Wraps `XTablePortable`; uses `ITransformer<T,IData>` for portable serialisation. |
| `XFile` | Wraps `XStream`; extends `Stream` on top of an `XTablePortable<long,byte[]>`. |

### 4.3 `Locator` — the Table Descriptor

File: `WaterfallTree/Locator.cs`  
A `Locator` is a **descriptor** for one table (one logical collection).
It carries:

- `Id` — unique `long` assigned by `Scheme.ObtainPathId()`.
- `Name`, `StructureType` (`XTABLE` or `XFILE`).
- `KeyDataType`, `RecordDataType` — schema of the data.
- `KeyType`, `RecordType` — the actual CLR types.
- `KeyComparer`, `KeyEqualityComparer` — built by `TypeEngine`.
- `KeyPersist`, `RecordPersist`, `KeyIndexerPersist`, `RecordIndexerPersist` — serialisation.
- `Apply` — the `IApply` implementation (`XTableApply` or `XStreamApply`).
- `OperationCollectionFactory`, `OrderedSetFactory` — factories for tree internals.

`Locator.Prepare()` lazily builds all serialisation and comparison objects via `TypeEngine`.

### 4.4 `Scheme`

File: `WaterfallTree/Scheme.cs`  
A `ConcurrentDictionary<long, Locator>` (id → locator).  
Persisted to `HANDLE_SCHEME` on every `Commit()`.

---

## 5. Data Layer

### 5.1 `IData` / `Data<T>`

All keys and records inside the engine travel as `IData`.  
`Data<T>` is a thin generic wrapper: `class Data<T> : IData { public T Value; }`.

### 5.2 `DataType`

File: `Data/DataType.cs`  
A structural type descriptor.  Supports:
- Primitives: `Boolean`, `Int32`, `Int64`, `Double`, `Decimal`, `String`, `DateTime`, …
- Composites: `Slotes(…)`, `Array(T)`, `List(T)`, `Dictionary(K,V)`.

Used to describe the schema of a table without referencing CLR types directly
(enables schema-first / portable tables).

### 5.3 `Slots<T0,T1,…>` — Composite Keys/Records

File: `Data/Slots.cs`  
Generic tuple-like structs for multi-column keys/records (up to 16 fields).
For >16 fields `SlotsBuilder` emits a new IL type at runtime.

```csharp
// 2-column key example
var table = engine.OpenXTable<Slots<int, string>, MyRecord>("t");
```

### 5.4 `TypeEngine`

File: `WaterfallTree/TypeEngine.cs`  
Cached factory (`ConcurrentDictionary<Type, TypeEngine>`) that creates, for a
given CLR type:
- `IComparer<IData>` via `DataComparer` (Expression-compiled comparator)
- `IEqualityComparer<IData>` via `DataEqualityComparer`
- `IPersist<IData>` via `DataPersist` (Expression-compiled reader/writer)
- `IIndexerPersist<IData>` via `DataIndexerPersist` (bulk column-oriented serialisation)

### 5.5 Expression-compiled Serialisation

All serialisation, comparison, and transformation code in `Data/` is built
**once** using `System.Linq.Expressions` and compiled to delegates.
No reflection at runtime — the delegates are cached as fields.

Key files:

| File | What it builds |
|------|----------------|
| `DataPersist.cs` | `Action<BinaryWriter,IData>` + `Func<BinaryReader,IData>` for any type |
| `DataIndexerPersist.cs` | Column-oriented bulk persist (used for vertical compression in `OrderedSetPersist`) |
| `IndexerPersist.cs` | Generic version of the above |
| `DataComparer.cs` | `IComparer<IData>` for any primitive-all type |
| `DataEqualityComparer.cs` | `IEqualityComparer<IData>` for any primitive-all type |
| `DataTransformer.cs` | `ITransformer<T,IData>` — converts user types ↔ anonymous `IData` |
| `DataToString.cs` | `IToString<IData>` |
| `DataToObjects.cs` | `IToObjects<IData>` |

### 5.6 `ITransformer<T,IData>`

Used by `XTablePortable<TKey,TRecord>` to convert between a typed user key/record
and the schema-typed `IData` stored in the engine.  Enables cross-language
or cross-version portable tables.

---

## 6. Storage (Heap)

### 6.1 `Heap` : `IHeap`

File: `Storage/Heap.cs`  
Block-based, handle-addressed storage on top of any `Stream`.

- Each logical block has a `long handle` (assigned by `ObtainNewHandle()`).
- Internally backed by `Space` (free-space manager) + `Pointer`/`Ptr` structs.
- Supports optional **Deflate compression** per block.
- **Atomic commit**: uses `AtomicHeader` (double-write to two positions) so a
  crash mid-commit always leaves at least one valid header.
- `Tag` byte array: small metadata area committed atomically with all block writes.

Fixed reserved handles:

| Handle | Content |
|--------|---------|
| 0 (`HANDLE_SETTINGS`) | WTree tuning parameters |
| 1 (`HANDLE_SCHEME`) | Serialised `Scheme` (all Locators) |
| 2 (`HANDLE_ROOT`) | Root branch cache |
| 3 (`HANDLE_RESERVED`) | Reserved |
| 4+ | Node data (assigned dynamically) |

### 6.2 `AllocationStrategy`

`Space` manages free extents. The strategy controls where new blocks are placed:
- `FromTheCurrentBlock` — pack tightly after the current position (default)
- other strategies available via the enum

### 6.3 `AtomicHeader`

File: `Storage/AtomicHeader.cs`  
Two-copy header written at fixed offsets.  On read, the valid copy is chosen
by comparing version numbers.  Guarantees crash-safe metadata.

---

## 7. Operations & Apply

The engine is **operation-sourced**: all mutations are represented as
`IOperation` objects collected in `IOperationCollection` and applied to
`IOrderedSet<IData,IData>` at leaves.

### 7.1 Operation Types

| Class | File | Scope | Code constant |
|-------|------|-------|---------------|
| `ReplaceOperation` | `PointOperations.cs` | Point | `REPLACE = 1` |
| `InsertOrIgnoreOperation` | `PointOperations.cs` | Point | `INSERT_OR_IGNORE = 2` |
| `DeleteOperation` | `PointOperations.cs` | Point | `DELETE = 3` |
| `DeleteRangeOperation` | `RangeOperations.cs` | Range | `DELETE_RANGE = 4` |
| `ClearOperation` | `OverallOperations.cs` | Overall | `CLEAR = 5` |

### 7.2 `IApply` Implementations

| Class | File | Used for |
|-------|------|----------|
| `XTableApply` | `Database/XTableApply.cs` | All `XTABLE` tables (key-value, sorted) |
| `XStreamApply` | `Database/XStreamApply.cs` | `XFILE` (byte-stream) tables |

`XTableApply.Leaf()` has three code paths ordered by speed:
1. **Sequential / monotone** — `UnsafeAdd` when all operations are REPLACE/INSERT
   on monotonically increasing keys into an empty leaf.
2. **Common-action** — when all operations in the batch have the same code.
3. **Standard** — general case with per-operation `switch`.

### 7.3 `OperationCollection`

File: `Database/OperationCollection.cs`  
Inherits `List<IOperation>`.  Tracks:
- `CommonAction` — if all operations have the same `Code`, set here; else `UNDEFINED`.
- `AreAllMonotoneAndPoint` — true when ops are all `Point` and in ascending key order.

Used by the sequential-apply fast path to skip the general loop.

### 7.4 `OrderedSet`

File: `General/Collections/OrderedSet.cs`  
Implements `IOrderedSet<TKey,TValue>`.  Backed by the project-owned `SortedSet<T>`
(not BCL's — this one exposes internal state via `SortedSetExtensions`).
All leaf data lives here.  Supports `Split(count)` and `Merge(set)` for
node splitting/merging, and bulk `LoadFrom(array, count, isOrdered)` for fast deserialisation.

---

## 8. Remote (TCP)

### 8.1 Architecture

```
Client process                      Server process
  StorageEngineClient               StorageEngineServer
    ClientConnection         TCP      TcpServer
      → serialize ICommand  ──────►  → deserialize
                                       → dispatch to StorageEngine / Heap
                                       → serialize response
      ◄─ deserialize ──────────────
    XTableRemote              wraps the remote table as ITable<IData,IData>
```

### 8.2 Key Files

| File | Role |
|------|------|
| `Remote/StorageEngineClient.cs` | `IStorageEngine` that speaks over TCP |
| `Remote/StorageEngineServer.cs` | Dispatches incoming `ICommand` packets to the local engine |
| `Remote/XTableRemote.cs` | `ITable<IData,IData>` that sends commands to the server |
| `Remote/Descriptor.cs` | Lightweight `IDescriptor` implementation used on the client |
| `Remote/Commands/` | Per-operation command objects + `CommandsPersist` |
| `Remote/Heap/RemoteHeap.cs` | `IHeap` that delegates to the server-side heap |
| `General/Communication/TcpServer.cs` | Non-blocking multi-client TCP listener |
| `General/Communication/ServerConnection.cs` | One client socket managed by the server |
| `General/Communication/ClientConnection.cs` | Client-side socket + send queue |

### 8.3 Entry Points

```csharp
// Server
var engine = Database.CatDb.FromFile("db.catdb");
var server = Database.CatDb.CreateServer(engine, port: 7182);
server.Start();

// Client
var engine = Database.CatDb.FromNetwork("localhost", 7182);
var table  = engine.OpenXTable<long, string>("tbl");
```

---

## 9. `Database.CatDb` Static Factory

File: `Database/CatDb.cs` — the public entry point for all usage.

```csharp
Database.CatDb.FromFile(fileName)    // production use
Database.CatDb.FromMemory()          // in-memory (tests, staging)
Database.CatDb.FromStream(stream)    // any Stream
Database.CatDb.FromHeap(heap)        // custom IHeap
Database.CatDb.FromNetwork(host, port) // remote client
Database.CatDb.CreateServer(engine, port) // TCP server wrapper
```

All return `IStorageEngine` (or `StorageEngineServer` for `CreateServer`).

---

## 10. General Utilities

| Namespace | Key classes |
|-----------|-------------|
| `General/Collections` | `OrderedSet<K,V>`, `IOrderedSet<K,V>`, `Cache<K,V>` (LRU) |
| `General/Compression` | `CountCompression` (variable-length uint), `DeltaCompression` |
| `General/Communication` | `TcpServer`, `ServerConnection`, `ClientConnection`, `Packet` |
| `General/IO` | `OptimizedFileStream` (buffered + lock-free reads), `AtomicFile` |
| `General/Persist` | `IPersist<T>`, `IIndexerPersist<T>`, and concrete implementations for all primitives |
| `General/Extensions` | `SortedSetExtensions` (exposes `SortedSet<T>` internals), `DecimalExtensions`, `ListExtensions`, etc. |
| `General/Comparers` | Byte-order-aware array comparers, `ComparerInvertor` |
| `General/Mathematics` | `MathUtils` |
| `General/Threading` | `Countdown` |
| `General/Diagnostics` | `MemoryMonitor` |

---

## 11. C# Style Conventions (post-refactor)

All source files follow the conventions applied in the 2026 modernisation pass:

- **File-scoped namespaces** (`namespace Foo.Bar;`) — no block wrapper.
- **Expression-bodied members** for single-line methods and properties.
- **Primary constructors** where the constructor only assigns fields.
- **`var`** for local variables when the type is obvious from the right-hand side.
- **Collection expressions** `[…]` instead of `new T[] {…}`.
- **`is not null` / `is null`** instead of `!= null` / `== null`.
- **`??=`** for null-coalescing assignment.
- **Modern `switch` expressions** instead of `switch` statements where a value is returned.
- **No dead commented-out code** — removed during refactor.
- **`ArgumentNullException.ThrowIfNull()`** instead of manual null-checks.
- No `#region` / `#endregion` blocks.

---

## 12. Known Sharp Edges & Gotchas

| Area | Issue |
|------|-------|
| `SortedSetExtensions.SortedSetHelper<T>` | Uses `Expression.Field` on **project-owned** `CatDb.SortedSet<T>` — works correctly. `SortedSetHelperAa<T>` in the same file targets BCL `SortedSet<T>` private fields — **dead code, do not use**. |
| `SlotsBuilder` | For >16 `Slots` fields emits a new type via `ILGenerator`. Fewer than 16 fields use the generic `Slots<T0,…>` classes. |
| `WTree.FindData` concurrency | Takes and releases `Monitor` locks on `Branch` objects during tree traversal. Never hold a branch lock and call user code. |
| `XTablePortable.Flush()` | Must be called before any read. `TryGet`, `Forward`, `Backward` call it automatically. |
| `StorageEngine.Commit()` | Not automatic — caller must invoke `engine.Commit()` to persist. Calling `Dispose()` / `Close()` does **not** commit. |
| `Thread.Abort()` | Removed everywhere. All thread shutdown uses `CancellationToken` + cooperative `Join()`. |
| Heap allocation strategy | Default is `FromTheCurrentBlock`. Only change for benchmarking; wrong strategy can fragment the file. |
| `OperationPersist.cs` / `ResultOperation.cs` | Both are empty stub files — remnants from STSdb4. Do not add code there. |

---

## 13. Test Suite Layout

```
CatDb.Tests/
  Data/
    ComparerTests.cs            — DataComparer for all primitive types
    EqualityComparerTests.cs    — DataEqualityComparer
    DecimalHelperTests.cs       — Decimal bits / GetDigits
    DataPersistTests.cs         — round-trip serialise / deserialise
    SlotsBuilderTests.cs        — Slots<> and ILGenerator path
  Database/
    TableCrudTests.cs           — Insert / Replace / Delete / Clear
    TableNavigationTests.cs     — Forward / Backward / FindNext / FindBefore …
    KeyValueTypeTests.cs        — various key & record CLR types
    PersistenceTests.cs         — Commit + reopen + verify data
    ConcurrencyTests.cs         — concurrent writers + readers
  Storage/
    HeapTests.cs                — Heap read/write/commit/reopen
```

All 133 tests must be green. Run `dotnet test CatDb.Tests` before committing.

---

## 14. Typical Change Checklist

When adding / modifying features:

1. **Does the change touch `WTree`?**  
   Check that `_internalNodeMaxOperationsInRoot`, overflow/underflow thresholds,
   and `Commit()` serialisation are consistent.

2. **Does the change add a new `IOperation` type?**  
   - Add constant in `OperationCode.cs`.
   - Add class in `PointOperations.cs`, `RangeOperations.cs`, or `OverallOperations.cs`.
   - Handle in `XTableApply.Leaf()` and/or `XStreamApply.Leaf()`.
   - Serialise/deserialise in `OperationCollectionPersist.cs`.

3. **Does the change add a new table type?**  
   - Add `StructureType` constant.
   - Add `IApply` implementation.
   - Register in `Locator` constructor `Apply = … switch`.
   - Add `Open…` method in `StorageEngine` and `IStorageEngine`.

4. **Does the change add a new remote command?**  
   - Add constant in `CommandCode.cs`.
   - Add `ICommand` class in `Remote/Commands/`.
   - Handle in `StorageEngineServer` dispatch array.
   - Serialise/deserialise in `CommandsPersist`.

5. **Always run `dotnet test CatDb.Tests` and confirm 133/133 green.**
- [ ] `CatDb/CatDb.csproj` → `net10.0`, remove outdated NuGet packages
- [ ] `CatDb.GettingStarted/CatDb.GettingStarted.csproj` → `net10.0`

---

### ✅ Phase 2 — Critical: Thread.Abort() (PlatformNotSupportedException on .NET 6+)
Files: `thread.Abort()` throws on all .NET Core/5+ runtimes. Fix = remove the Abort call (CancellationToken cooperative cancellation already in place).
- [ ] `Remote/StorageEngineServer.cs` line 97
- [ ] `Remote/Heap/HeapServer.cs` line 51
- [ ] `General/Communication/TcpServer.cs` line 54
- [ ] `General/Communication/ServerConnection.cs` lines 58, 67
- [ ] `General/Communication/ClientConnection.cs` lines 71, 78 (also has inverted `if (Join)` bug)

---

### ✅ Phase 3 — CodeDom / Reflection.Emit Cleanup
**`Data/SlotsBuilder.cs`**
- [ ] Remove `BuildTypeCodeDom()` — dead Mono path, uses `System.CodeDom` + `CSharpCodeProvider` which don't work on .NET Core
- [ ] Remove Mono branch from `BuildType(Type baseInterface, ...)` — always use `BuildTypeEmit`
- [ ] Remove `using System.CodeDom`, `using System.CodeDom.Compiler`, `using Microsoft.CSharp`
- [ ] `BuildTypeEmit` (ILGenerator for >16 slots) — kept, as Expression trees cannot create new type definitions; this is the correct tool
- [ ] Remove `System.CodeDom 6.0.0` NuGet package from csproj
- [ ] Remove `System.Reflection.Emit 4.7.0` NuGet package (inbox in .NET 6+)

---

### ✅ Phase 4 — Expression Builder: Replace Private Reflection
**`General/Extensions/DecimalExtensions.cs`**
- [ ] `CreateConstructorMethod()`: Replace `GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, ...)` targeting the internal `decimal(int,int,int,int)` ctor with `Expression.New(typeof(decimal).GetConstructor(new[]{typeof(int[])}), Expression.NewArrayInit(...))` — public stable API
- [ ] Remove `#if NETFX_CORE` / `#else` / `#endif` dead preprocessor blocks
- [ ] Remove `using System.Reflection` once BindingFlags no longer used

**`General/Extensions/SortedSetExtensions.cs`** (future phase)
- `SortedSetHelperAa<T>` class uses `Expression.Field(set, "root")` / `"count"` / `"version"` / `"comparer"` — private BCL `System.Collections.Generic.SortedSet<T>` fields
- `GetMethod("FindNode", BindingFlags.NonPublic)`, `GetMethod("ConstructRootFromSortedArray")` etc — private BCL methods
- Status: `SortedSetHelper<T>` (the actual used class) works correctly with the **project-owned** `CatDb.SortedSet<T>` (which has `internal` fields). The `SortedSetHelperAa<T>` class is unused/WIP dead code.
- Action needed: verify `SortedSetHelper<T>` uses `CatDb.SortedSet<T>` only → then mark safe. Remove or fix `SortedSetHelperAa<T>`.

---

### ✅ Phase 5 — Hardening: Cache Reflection Lookups
**`Data/Comparer.cs`** lines 124, 132, 218
- `GetMethod()` calls inside expression-tree builders run on every instantiation → cache as `static readonly MethodInfo`
**`Data/EqualityComparer.cs`** lines 125, 131, 147, 200, 206
- Same pattern → cache as `static readonly MethodInfo`

---

## Files Changed

| File | Change | Status |
|------|--------|--------|
| `CatDb/CatDb.csproj` | net6.0 → net10.0, remove CodeDom + Emit NuGet pkgs | ✅ |
| `CatDb.GettingStarted/CatDb.GettingStarted.csproj` | net6.0 → net10.0 | ✅ |
| `Remote/StorageEngineServer.cs` | Remove Thread.Abort() | ✅ |
| `Remote/Heap/HeapServer.cs` | Remove Thread.Abort() | ✅ |
| `General/Communication/TcpServer.cs` | Remove Thread.Abort() | ✅ |
| `General/Communication/ServerConnection.cs` | Remove Thread.Abort() | ✅ |
| `General/Communication/ClientConnection.cs` | Remove Thread.Abort(), fix inverted Join condition | ✅ |
| `Data/SlotsBuilder.cs` | Remove BuildTypeCodeDom + Mono branch + dead usings (CodeDom/CSharpCodeProvider/Microsoft.CSharp) | ✅ |
| `General/Extensions/DecimalExtensions.cs` | Replace NonPublic ctor with Expression.New(decimal(int[])), remove #if NETFX_CORE, remove BindingFlags | ✅ |
| `General/IO/AtomicFile.cs` | Stream.Read → ReadExactly (CA2022) | ✅ |
| `Storage/Heap.cs` | DeflateStream.Read → ReadExactly (CA2022) | ✅ |

---

## Build Status
- Baseline (net6.0): ✅ 0 errors, 0 warnings
- After migration (net10.0): ✅ 0 errors, 0 warnings

## Test Status  
- GettingStarted (1M records): ✅ Insert ~280,033 rec/sec | Read ~776,397 rec/sec | Value lookups correct
- Unit test suite (xUnit): ✅ **133/133 passing** (`CatDb.Tests` project)

### Unit Test Coverage
| File | Tests | Status |
|------|-------|--------|
| `Data/ComparerTests.cs` | 10 | ✅ All pass |
| `Data/EqualityComparerTests.cs` | 10 | ✅ All pass |
| `Data/DecimalHelperTests.cs` | 10 | ✅ All pass |
| `Data/DataPersistTests.cs` | ~14 | ✅ All pass |
| `Data/SlotsBuilderTests.cs` | ~5 | ✅ All pass |
| `Database/TableCrudTests.cs` | ~14 | ✅ All pass |
| `Database/TableNavigationTests.cs` | ~12 | ✅ All pass |
| `Database/KeyValueTypeTests.cs` | ~14 | ✅ All pass |
| `Database/PersistenceTests.cs` | ~6 | ✅ All pass |
| `Database/ConcurrencyTests.cs` | ~3 | ✅ All pass |
| `Storage/HeapTests.cs` | ~7 | ✅ All pass |

### Bugs Found and Fixed by Unit Tests
| Bug | Fix | File |
|-----|-----|------|
| `DecimalHelper.GetDigits` used private `_flags` field (now `uint` in .NET 10) | Use `Decimal.GetBits()` public API with local variable to dereference `ref decimal` | `General/Extensions/DecimalExtensions.cs` |
| `SortedSetHelper.GetViewBetween` copy-paste: `lowerBoundActive` passed twice instead of `upperBoundActive` — caused NullReferenceException and wrong navigation results | Change last arg to `upperBoundActive` | `General/Extensions/SortedSetExtensions.cs` |
| `GetConstructor(new Type[] {})` returns null for structs without explicit parameterless constructor in .NET 10 — caused `ArgumentNullException: constructor` when using struct types as table keys | Replace `Expression.New(T.GetConstructor(...))` with `Expression.New(T)` for value types | `Data/DataIndexerPersist.cs`, `Data/IndexerPersist.cs`, `Data/ValueToObjects.cs`, `Data/ValueToString.cs`, `Data/DataToString.cs`, `Data/DataToObjects.cs`, `Data/DataTransformer.cs` |

---

## Remaining Future Work (next session)

### Phase 5 — Hardening: Cache Reflection Lookups
**`Data/Comparer.cs`** lines 124, 132, 218 — `GetMethod()` calls inside expression-tree builders, uncached
**`Data/EqualityComparer.cs`** lines 125, 131, 147, 200, 206 — same pattern
Fix: extract all `typeof(X).GetMethod(...)` lookups into `static readonly MethodInfo` fields

### Phase 6 — SortedSetHelperAa dead code
`General/Extensions/SortedSetExtensions.cs` — `SortedSetHelperAa<T>` class is unused WIP code that
targets BCL's `System.Collections.Generic.SortedSet<T>` private fields via `Expression.Field(set, "root")` etc.
`SortedSetHelper<T>` (the actually-used class) works correctly with project-owned `CatDb.SortedSet<T>`.
Action: verify, then either delete `SortedSetHelperAa<T>` or complete its migration.
