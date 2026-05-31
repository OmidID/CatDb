---
id: index-and-search
title: Index and Search
---

# Index and search

Every CatDb table is ordered by its primary key — the key *is* the index.
The fluent query builder gives you a consistent API for both primary-key range
scans and secondary index lookups.

All queries are **lazy**: no work happens until you enumerate the result
(`foreach`, `.ToList()`, `.FirstOrDefault()`) or call a terminal method
(`.Count()`, `.Exists()`).

```csharp
using CatDb.Extensions;
```

---

## Primary key queries

Call `table.Query()` with no arguments to start a primary-key builder,
then chain bound methods and enumerate.

```csharp
// All records with key >= 100
var rows = table.Query().AtLeast(100).ToList();

// Keys in the range [100, 200]
var range = table.Query().AtLeast(100).AtMost(200).ToList();

// Keys > 500 (exclusive lower bound)
var newer = table.Query().GreaterThan(500).ToList();

// Descending — last 5 records
var last5 = table.Query().Backward().Take(5).ToList();

// String prefix
var byPrefix = table.Query().StartsWith("ada").ToList();

// Count without materializing records
long n = table.Query().AtLeast(100).AtMost(200).Count();
```

### Builder methods

| Method | Meaning |
| --- | --- |
| `.AtLeast(from)` | Keys `>= from` |
| `.GreaterThan(from)` | Keys `> from` (exclusive) |
| `.AtMost(to)` | Keys `<= to` |
| `.LessThan(to)` | Keys `< to` (exclusive) |
| `.Between(from, to)` | Both bounds inclusive by default |
| `.Between(from, to, fromInclusive, toInclusive)` | Fine-grained inclusivity |
| `.Backward()` | Scan in descending key order |
| `.Take(n)` | Stop after *n* records |
| `.Skip(n)` | Skip first *n* records (prefer cursor paging for deep pages) |
| `.Where(predicate)` | Post-scan key filter |
| `.Count()` | Count matching records |
| `.StartsWith(prefix)` | String key prefix scan (extension — see below) |

### Key types

Keys are not limited to primitives. Any type with a registered comparer works —
`long`, `string`, `Guid`, composite `Slots<>`, or your own class.

```csharp
// Composite key example
var table = engine.OpenXTable<Slots<string, int>, Order>("orders");
var results = table.Query().AtLeast(new Slots<string, int>("acme", 0))
                           .AtMost(new Slots<string, int>("acme", int.MaxValue))
                           .ToList();
```

---

## Secondary index queries

Create a secondary index on any field, then query it with the same
fluent style — no manual generic type arguments required.

### Creating indexes

```csharp
var table = engine.OpenXTable<long, Customer>("customers");

// Unique index via lambda
table.CreateIndex("Email", c => c.Email, IndexType.Unique);

// Non-unique index
table.CreateIndex("City", c => c.City, IndexType.NonUnique);

// Composite index (field names)
table.CreateIndex("CityAge", new[] { "City", "Age" }, IndexType.NonUnique);
```

### Lookup by equality

```csharp
// Exact match on a unique index
var customer = table.Query(c => c.Email).Equals("ada@example.com").FirstOrDefault();

// All customers in a city
var inLondon = table.Query(c => c.City).Equals("London").ToList();

// Count without fetching records
long count = table.Query(c => c.City).Equals("London").Count();

// Check existence
bool exists = table.Query(c => c.Email).Equals("old@example.com").Exists();
```

### Range scan on an index

```csharp
// All emails starting with "ada"
var results = table.Query(c => c.Email).StartsWith("ada").ToList();

// Emails in a range
var emails = table.Query(c => c.Email)
    .AtLeast("a@example.com")
    .AtMost("b@example.com")
    .ToList();

// Numeric range on a field
var young = table.Query(c => c.Age).Between(18, 25).ToList();

// Limit results
var top10 = table.Query(c => c.City).Equals("NYC").Take(10).ToList();
```

### Index builder methods

| Method | Meaning |
| --- | --- |
| `.Equals(value)` | Exact field match |
| `.AtLeast(from)` | Field values `>= from` |
| `.GreaterThan(from)` | Field values `> from` |
| `.AtMost(to)` | Field values `<= to` |
| `.LessThan(to)` | Field values `< to` |
| `.Between(from, to)` | Inclusive range |
| `.Take(n)` | Limit results |
| `.Count()` | Count matches |
| `.Exists()` | Returns `true` if at least one match exists |
| `.StartsWith(prefix)` | String field prefix (extension) |

### Unique constraint

Writing a record that would duplicate a unique index value throws
`UniqueIndexViolationException`.

```csharp
try
{
    table.Replace(99, new Customer { Email = "ada@example.com" });
}
catch (UniqueIndexViolationException ex)
{
    Console.WriteLine($"Duplicate: index '{ex.IndexName}'");
}
```

### Index maintenance

Indexes are updated automatically on every `Replace` and `Delete` call.
Always `Commit()` in the same unit of work as your writes.

```csharp
table.Replace(42, updatedCustomer);  // index updated here
engine.Commit();                     // flushed to disk
```

---

## Prefix search

`StartsWith` works for both primary-key and index queries when the field
type is `string`. It computes the exclusive upper bound from the prefix so
only the matching leaf nodes are loaded.

```csharp
// Primary key prefix
var names = engine.OpenXTable<string, long>("name-to-id");
foreach (var row in names.Query().StartsWith("ada"))
    Console.WriteLine($"{row.Key} -> {row.Value}");

// Index field prefix
foreach (var row in table.Query(c => c.Email).StartsWith("ada"))
    Console.WriteLine(row.Value.Name);
```

---

## Count

```csharp
// Primary key range count
long n = table.Query().Between(100, 200).Count();

// Full table count
long total = table.Query().Count();

// Index count
long londonCount = table.Query(c => c.City).Equals("London").Count();
```

For local `XTable` / `XTablePortable` tables without a `.Where()` filter,
CatDb counts bounded ranges using leaf-level index arithmetic instead of
materializing every record.

---

## Paging

Prefer **keyset paging** for deep pages — it is O(log n) per page:

```csharp
var query = table.Query().AtLeast(0L);

var page1 = table.PageAfter(query.BuildKeyQuery(), take: 100).ToList();
var page2 = table.PageAfter(query.BuildKeyQuery(),
                             afterKey: page1.Last().Key, take: 100).ToList();
```

Or use the builder's `.After()` shorthand:

```csharp
var page2 = table.Query().AtLeast(0L).After(page1.Last().Key).Take(100).ToList();
```

**Offset paging** is available but scans every skipped row:

```csharp
var page = table.Query().AtLeast(0L).Skip(10_000).Take(100).ToList();
```

