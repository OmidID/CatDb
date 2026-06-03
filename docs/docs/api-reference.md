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

Import `CatDb.Extensions`. All queries are lazy — execution defers until enumeration.

### Primary key builder

```csharp
// Start a primary-key range query
TableQuery<TKey, TRecord> q = table.Query();

// Chain bounds
q.AtLeast(from)
q.GreaterThan(from)
q.AtMost(to)
q.LessThan(to)
q.Between(from, to)
q.Between(from, to, fromInclusive: false, toInclusive: false)

// Modifiers
q.Backward()           // descending scan
q.Take(n)              // limit
q.Skip(n)              // offset (prefer cursor paging for deep pages)
q.Where(predicate)     // post-scan key filter
q.After(key)           // cursor — exclusive lower bound for next page

// Terminals
q.Count()              // count without materializing records
q.ToList()             // enumerate

// String-key extension
q.StartsWith(prefix)
```

### Secondary index builder

```csharp
// Start an index query via lambda (index name resolved automatically)
IndexQuery<TKey, TRecord, TField> iq = table.Query(c => c.Email);

// Start via explicit index name
IndexQuery<TKey, TRecord, TField> iq = table.Query<TKey, TRecord, string>("Email");

// Criteria
iq.Equals(value)
iq.AtLeast(from)
iq.GreaterThan(from)
iq.AtMost(to)
iq.LessThan(to)
iq.Between(from, to)

// Modifiers
iq.Take(n)

// Terminals
iq.Count()
iq.Exists()
iq.ToList()

// String-field extension
iq.StartsWith(prefix)
```

### Backward-compatible helpers

The following extension methods still exist and delegate to the builder:

```csharp
table.QueryTake(keyQuery, take);
table.QueryBackward(keyQuery);
table.QueryBackwardTake(keyQuery, take);
table.Count(keyQuery);
table.Page(keyQuery, skip, take);
table.PageAfter(keyQuery, take);
table.PageAfter(keyQuery, afterKey, take);
```

## Async extensions

`CatDb.Extensions.TableAsyncExtensions` wraps synchronous table operations in `Task.Factory.StartNew`. These helpers are useful for caller ergonomics, but they do not make the underlying engine I/O natively asynchronous.

```csharp
await table.ReplaceAsync(1, "value", cancellationToken);
var value = await table.TryGetAsync(1, cancellationToken);
var count = await table.CountAsync(cancellationToken);
```
