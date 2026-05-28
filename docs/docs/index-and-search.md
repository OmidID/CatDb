---
id: index-and-search
title: Index and Search
---

# Index and search

Every CatDb table is ordered by its key. The key is the index.

CatDb does not maintain separate secondary indexes in the current public API. If you need another lookup path, create another table whose key is the alternate access pattern and whose value points back to the primary record.

## KeyQuery

`KeyQuery<TKey>` describes bounded scans over the sorted key space.

```csharp
using CatDb.Extensions;

var rows = table.Query(KeyQuery<long>.Between(100, 200));
var newer = table.Query(KeyQuery<long>.GreaterThan(500));
var older = table.QueryBackward(KeyQuery<long>.AtMost(500));
```

| Factory | Meaning |
| --- | --- |
| `KeyQuery<TKey>.All()` | All records. |
| `AtLeast(from)` | Keys `>= from`. |
| `GreaterThan(from)` | Keys `> from`. |
| `AtMost(to)` | Keys `<= to`. |
| `LessThan(to)` | Keys `< to`. |
| `Between(from, to, fromInclusive, toInclusive)` | Bounded range. |
| `WithFilter(predicate)` | Applies a key predicate after the indexed range scan. |
| `KeyQuery.StartsWith(prefix)` | String prefix search with an exclusive computed upper bound. |

## Prefix search

```csharp
var names = engine.OpenXTable<string, long>("name-to-id");

foreach (var row in names.Query(KeyQuery.StartsWith("ada")))
{
    Console.WriteLine($"{row.Key} -> {row.Value}");
}
```

`StartsWith` computes the first string greater than the prefix range, then passes both bounds into the WTree scan. That avoids loading leaves after the prefix range.

## Counts

```csharp
long count = table.Count(KeyQuery<long>.Between(100, 200));
```

For local `XTable`/`XTablePortable` tables without a filter, CatDb can count bounded ranges with leaf-level index arithmetic instead of materializing every record.

## Paging

Prefer keyset paging for deep pages:

```csharp
var query = KeyQuery<long>.AtLeast(0);
var page1 = table.PageAfter(query, take: 100).ToList();
var page2 = table.PageAfter(query, afterKey: page1.Last().Key, take: 100).ToList();
```

Offset paging is available but scans the skipped rows:

```csharp
var page = table.Page(query, skip: 10_000, take: 100);
```

## Secondary index pattern

```csharp
var users = engine.OpenXTable<long, User>("users");
var usersByEmail = engine.OpenXTable<string, long>("users_by_email");

users[42] = new User("ada@example.com", "Ada");
usersByEmail["ada@example.com"] = 42;

engine.Commit();

if (usersByEmail.TryGet("ada@example.com", out var id))
{
    var user = users[id];
}

public sealed record User(string Email, string Name);
```

Keep secondary index tables updated in the same unit of work before `Commit()`.
