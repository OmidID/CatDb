---
id: architecture
title: Architecture
---

# Architecture

CatDb layers a typed table API over a WTree and a heap.

```text
User API
  ITable<TKey,TRecord>, XTable<TKey,TRecord>, XFile

Storage engine
  StorageEngine, Locator, Scheme

Waterfall Tree
  WTree, Branch, InternalNode, LeafNode, BranchCache

Heap
  WalHeap, Heap, Space, Pointer, OptimizedFileStream

Data layer
  IData, Data<T>, DataType, persist/compare/transform helpers
```

## Key files

| File | Role |
| --- | --- |
| `CatDb/Database/StorageEngine.cs` | Public storage engine implementation. |
| `CatDb/Database/ITable.cs` | Main table contract. |
| `CatDb/Database/XTable.cs` | Typed table wrapper. |
| `CatDb/Database/XTablePortable.cs` | Internal portable table implementation. |
| `CatDb/Database/XStream.cs` | Stream backed by a table of byte blocks. |
| `CatDb/WaterfallTree/WTree.cs` | Core WTree execution, lookup, commit, and cache eviction. |
| `CatDb/WaterfallTree/WTree.Branch.Fall.cs` | Cascades buffered operations down the tree. |
| `CatDb/WaterfallTree/WTree.InternalNode.cs` | Routes operations and searches to child branches. |
| `CatDb/WaterfallTree/WTree.InternalNode.Maintenance.cs` | Node split, merge, and branch rebuild logic. |
| `CatDb/Storage/WalHeap.cs` | Crash-safe heap wrapper. |

## Write path

```text
table[key] = record
  -> XTable.Replace
  -> XTablePortable.Execute(operation)
  -> WTree.Execute(operation collection)
  -> root branch cache
  -> Fall when thresholds are exceeded
  -> leaf ordered set
```

The WTree batches operations in branch caches so random writes can be applied in larger ordered chunks.

## Read path

```text
table.TryGet(key)
  -> table Flush
  -> WTree.FindData(locator, key)
  -> top-down branch traversal
  -> leaf ordered set lookup
```

Range scans use the same ordered tree structure, seeking to a lower bound and then walking leaf data in order.

## Concurrency model

- Root operations serialize with the root branch monitor.
- Branch locking uses `Monitor`/`lock(this)` and is reentrant per thread.
- Traversal is top-down: root, child, grandchild.
- Code must not acquire locks upward from a child to a parent.
- Cache eviction runs synchronously during commit.

## Data model

User keys and records become `IData` internally. `DataType`, `DataPersist`, `DataComparer`, and `DataTransformer` build comparison and serialization delegates from CLR types or schema descriptors.

The locator stores the schema and all generated helpers needed for one logical structure.
