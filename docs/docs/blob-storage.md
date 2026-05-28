---
id: blob-storage
title: Blob Storage
---

# Blob storage

CatDb exposes blob-like storage through `XFile`, a `Stream` implementation backed by an internal table. Use it when you want random-access bytes stored in the same database engine.

```csharp
using var engine = CatDb.Database.CatDb.FromFile("files.catdb");

using var file = engine.OpenXFile("avatar");
var bytes = File.ReadAllBytes("avatar.png");
file.Write(bytes, 0, bytes.Length);

engine.Commit();
```

## How XFile stores bytes

`XFile` inherits from `XStream`. The stream stores bytes in an `XTABLE` whose key type is `long` and whose record type is `byte[]`.

| Detail | Value |
| --- | --- |
| Structure type | `XFILE` |
| Internal key | `long` byte offset |
| Internal record | `byte[]` block |
| Block size | `2 KiB` |

## Read and seek

```csharp
using var file = engine.OpenXFile("avatar");

file.Seek(0, SeekOrigin.Begin);

var buffer = new byte[file.Length];
var read = file.Read(buffer, 0, buffer.Length);
```

The stream supports:

- `Read`
- `Write`
- `Seek`
- `SetLength`
- `Position`
- `Length`

`Flush()` on `XStream` is a no-op. Durability still comes from `engine.Commit()`.

## Zero and truncate

`SetLength` either extends the file by writing a zero byte at the new end or truncates by deleting affected internal ranges.

```csharp
file.SetLength(0);
engine.Commit();
```

## When to use a table instead

Use a normal table when records have natural keys or fields you need to search. Use `XFile` when the primary access pattern is stream-like byte reads and writes.
