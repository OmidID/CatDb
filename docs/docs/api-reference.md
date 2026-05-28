---
id: api-reference
title: API Reference
---

# API reference

This page summarizes the public APIs most users touch first.

## `CatDb.Database.CatDb`

```csharp
IStorageEngine FromFile(string fileName, DatabaseOptions? options = null);
IStorageEngine FromMemory(DatabaseOptions? options = null);
IStorageEngine FromStream(Stream stream, DatabaseOptions? options = null);
IStorageEngine FromHeap(IHeap heap, DatabaseOptions? options = null);
IStorageEngine FromNetwork(string host, int port = 7182, string databaseName = "default", string? userName = null, string? password = null);
Task<IStorageEngine> FromNetworkAsync(...);
StorageEngineServer CreateServer(IStorageEngine engine, int port = 7182);
```

## `IStorageEngine`

```csharp
ITable<TKey, TRecord> OpenXTable<TKey, TRecord>(string name);
ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name);
ITable<IData, IData> OpenXTablePortable(string name, DataType keyDataType, DataType recordDataType);
XFile OpenXFile(string name);

void Delete(string name);
void Rename(string name, string newName);
bool Exists(string name);
void Commit();
void Close();
```

`IStorageEngine` is enumerable. Enumerating it returns table and file descriptors.

## `ITable<TKey, TRecord>`

```csharp
TRecord this[TKey key] { get; set; }

void Replace(TKey key, TRecord record);
void InsertOrIgnore(TKey key, TRecord record);
void Delete(TKey key);
void Delete(TKey fromKey, TKey toKey);
void Clear();

bool Exists(TKey key);
bool TryGet(TKey key, out TRecord? record);
TRecord? Find(TKey key);
TRecord TryGetOrDefault(TKey key, TRecord defaultRecord);

KeyValuePair<TKey, TRecord>? FindNext(TKey key);
KeyValuePair<TKey, TRecord>? FindAfter(TKey key);
KeyValuePair<TKey, TRecord>? FindPrev(TKey key);
KeyValuePair<TKey, TRecord>? FindBefore(TKey key);

IEnumerable<KeyValuePair<TKey, TRecord>> Forward();
IEnumerable<KeyValuePair<TKey, TRecord>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo);
IEnumerable<KeyValuePair<TKey, TRecord>> Backward();
IEnumerable<KeyValuePair<TKey, TRecord>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom);

long Count();
KeyValuePair<TKey, TRecord>? FirstRow { get; }
KeyValuePair<TKey, TRecord>? LastRow { get; }
```

## Query extensions

Import `CatDb.Extensions`.

```csharp
table.Query(query);
table.QueryTake(query, take);
table.QueryBackward(query);
table.QueryBackwardTake(query, take);
table.Count(query);
table.Page(query, skip, take);
table.PageAfter(query, take);
table.PageAfter(query, afterKey, take);
```

## Async extensions

`CatDb.Extensions.TableAsyncExtensions` wraps synchronous table operations in `Task.Factory.StartNew`. These helpers are useful for caller ergonomics, but they do not make the underlying engine I/O natively asynchronous.

```csharp
await table.ReplaceAsync(1, "value", cancellationToken);
var value = await table.TryGetAsync(1, cancellationToken);
var count = await table.CountAsync(cancellationToken);
```
