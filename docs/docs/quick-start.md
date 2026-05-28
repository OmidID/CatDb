---
id: quick-start
title: Quick Start
---

# Quick start

CatDb is a .NET library. Add the package, open an engine, open a table, write records, and call `Commit()` when those writes must survive process exit.

```bash
dotnet add package CatDb
```

```csharp
using CatDb.Database;

using var engine = CatDb.Database.CatDb.FromFile("app.catdb");

var users = engine.OpenXTable<long, User>("users");
users[1] = new User("Ada", "Lovelace");
users[2] = new User("Grace", "Hopper");

engine.Commit();

if (users.TryGet(1, out var user))
{
    Console.WriteLine($"{user.FirstName} {user.LastName}");
}

public sealed record User(string FirstName, string LastName);
```

## Open an engine

```csharp
using var fileEngine = CatDb.Database.CatDb.FromFile("app.catdb");
using var memoryEngine = CatDb.Database.CatDb.FromMemory();
using var streamEngine = CatDb.Database.CatDb.FromStream(stream);
```

`FromFile` uses `CommitMode.WriteAheadLog` by default. It creates a main database file and a `.wal` file next to it.

## Open a table

```csharp
var table = engine.OpenXTable<long, string>("events");

table[100] = "created";
table.Replace(101, "updated");
table.InsertOrIgnore(101, "ignored because key exists");
table.Delete(100);

engine.Commit();
```

Tables are ordered by key, so scans return stable key order:

```csharp
foreach (var row in table.Forward())
{
    Console.WriteLine($"{row.Key}: {row.Value}");
}

foreach (var row in table.Backward())
{
    Console.WriteLine($"{row.Key}: {row.Value}");
}
```

## Use range queries

```csharp
using CatDb.Extensions;

var range = KeyQuery<long>.Between(100, 200);
var rows = table.Query(range);

var firstPage = table.PageAfter(range, take: 50).ToList();
var nextPage = table.PageAfter(range, firstPage.Last().Key, take: 50).ToList();
```

For string keys, prefix search is index-backed:

```csharp
var names = engine.OpenXTable<string, long>("name-to-id");
var matches = names.Query(KeyQuery.StartsWith("ada"));
```

## Build and run examples from source

```bash
cd src
dotnet build --no-incremental
dotnet test --no-build
dotnet run --project CatDb.GettingStarted
```

The Getting Started project contains runnable demos for basic insert/read, `KeyQuery`, range-count performance, and keyset paging.
