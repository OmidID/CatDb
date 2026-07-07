---
id: welcome
title: Welcome
slug: /
---

# Welcome to CatDb

CatDb is a high-performance ordered key-value database engine written in C# for modern .NET. It stores named tables in one database file, keeps keys sorted, and uses a Waterfall Tree (WTree) to batch and cascade writes through the tree.

You can use CatDb in three deployment modes:

- As a dedicated server via `CatDb.Server` (HTTP/TCP APIs, ASP.NET Core hosted), reached from client code with `CatDb.Database.CatDb.FromNetwork(...)` or one `FromConnectionString("Provider=Network;...")`.
- Directly in-process with `CatDb.Database.CatDb.FromFile("app.catdb")`.
- In-memory with `FromMemory()` for tests and short-lived workloads.

## What CatDb is good at

- Ordered key-value tables where keys are naturally searchable or range-scanned.
- Local persistence with explicit commits and a crash-safe write-ahead log by default.
- Simple server-side browsing and administration over HTTP.

## Main concepts

| Concept | Meaning |
| --- | --- |
| Storage engine | The top-level database object. It owns tables, files, commits, and the WTree. |
| Table | A named ordered key-value collection exposed through `ITable<TKey, TRecord>`. |
| XTable | The direct typed table wrapper around the internal portable table. |
| XFile | A `Stream` abstraction backed by an internal table of byte blocks. |
| WTree | The write-buffered ordered tree that stores all table data. |
| Commit | The explicit durability boundary. Closing an engine does not commit pending changes. |

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/CatDb` | Core library: storage engine, WTree, data layer, heap, remote TCP support. |
| `src/CatDb.Server` | ASP.NET Core server, health checks, authentication, admin/data HTTP APIs. |
| `tests/CatDb.Tests` | Unit tests for data, storage, persistence, CRUD, navigation, and concurrency. |
| `examples/CatDb.GettingStarted` | Runnable examples for inserts, reads, range queries, and paging. |
| `examples/CatDb.StressTest` | Long-running concurrent stress harness. |
| `examples/CatDb.CrashWriter` | Out-of-process crash-recovery test harness. |

Start with [Quick start](./quick-start), then read [Database Engine](./database-engine) when you want to understand how the WTree and heap fit together.
