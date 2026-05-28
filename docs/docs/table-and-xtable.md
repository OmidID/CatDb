---
id: table-and-xtable
title: Table and XTable
---

# Table and XTable

CatDb tables implement `ITable<TKey, TRecord>`. Keys are ordered, records are stored by key, and scans enumerate sorted key-value pairs.

## Open table variants

```csharp
var direct = engine.OpenXTable<long, Tick>("ticks");
var portable = engine.OpenXTablePortable<long, Tick>("ticks_portable");
```

| API | Description |
| --- | --- |
| `OpenXTable<TKey, TRecord>` | Direct typed wrapper. Uses `Data<TKey>` and `Data<TRecord>` internally. |
| `OpenXTablePortable<TKey, TRecord>` | Uses transformers and schema-first `DataType` descriptions. |
| `OpenXTablePortable(name, keyDataType, recordDataType)` | Low-level `ITable<IData, IData>` table. |

The first open of a table fixes its structure. Reopening the same name with a different key type, record type, or structure kind throws an argument exception.

## CRUD operations

```csharp
var table = engine.OpenXTable<int, string>("events");

table[1] = "created";                 // replace or insert
table.Replace(2, "updated");          // replace or insert
table.InsertOrIgnore(2, "ignored");   // keep existing value

bool exists = table.Exists(1);
bool found = table.TryGet(1, out var value);
string fallback = table.TryGetOrDefault(999, "missing");

table.Delete(1);
table.Delete(10, 20);                  // inclusive range delete
table.Clear();

engine.Commit();
```

## Navigation

```csharp
var first = table.FirstRow;
var last = table.LastRow;

var next = table.FindNext(100);     // nearest row at or after key
var after = table.FindAfter(100);   // nearest row strictly after key
var prev = table.FindPrev(100);     // nearest row at or before key
var before = table.FindBefore(100); // nearest row strictly before key
```

## Scans

```csharp
foreach (var row in table.Forward())
{
    Console.WriteLine(row.Key);
}

foreach (var row in table.Forward(from: 100, hasFrom: true, to: 200, hasTo: true))
{
    Console.WriteLine(row.Key);
}

foreach (var row in table.Backward())
{
    Console.WriteLine(row.Key);
}
```

Reads call into table flush paths before navigating the WTree, so pending table operations are visible to reads in the same engine.

## Composite and complex records

CatDb builds data schemas from CLR types. It supports primitives, common value types, arrays, lists, dictionaries, and composite slot types.

```csharp
using CatDb.Data;

var composite = engine.OpenXTable<Slots<int, string>, Tick>("ticks_by_symbol");
composite[new Slots<int, string>(2026, "MSFT")] = tick;
```

For portable tables, `DataTransformer<T>` maps user objects to schema-shaped `IData` values.
