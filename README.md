# CatDb

[<img src="https://raw.githubusercontent.com/OmidID/CatDb/master/images/nuget.png" align="left" width="128">](https://github.com/OmidID/CatDb/)

CatDb is an embedded ordered key-value database for .NET, built around the Waterfall Tree (WTree): https://ieeexplore.ieee.org/document/6857846.

CatDb started from STSdb4, but this project has evolved substantially and now differs significantly in architecture, behavior, and public API.

## Deployment Model

CatDb is **embedded-first**. The primary model is linking the engine directly into your .NET process and operating on local storage.

CatDb also includes optional remote/server components for specific scenarios, but those are wrappers over the same engine, not a separate server-oriented architecture.

## Installation

| Package | Description | Version |
|:-|:-|:-|
| [CatDb](https://www.nuget.org/packages/CatDb/) | Database engine library | ![Nuget](https://badgen.net/nuget/v/CatDb) |

## Why CatDb

- Embedded engine with no external service dependency.
- Ordered key storage with efficient forward/backward/range scans.
- Write-optimized WTree internals for heavy mixed workloads.
- Secondary index support in the public API.
- Fluent query builders for key and index queries.
- Supports non-primitive keys/fields (including composite/object types with comparers).

## Quick Start

```csharp
using CatDb.Database;

using var engine = CatDb.Database.CatDb.FromFile("app.catdb");
var users = engine.OpenXTable<long, User>("users");

users[1] = new User("ada@example.com", "Ada", "London");
users[2] = new User("grace@example.com", "Grace", "New York");

engine.Commit();

if (users.TryGet(1, out var user))
	Console.WriteLine($"{user.Email} / {user.City}");

public sealed record User(string Email, string Name, string City);
```

## Fluent Queries

```csharp
using CatDb.Extensions;

// Primary key query
var page = users.Query().AtLeast(1).Take(100).ToList();

// Descending query
var last10 = users.Query().Backward().Take(10).ToList();
```

## Secondary Indexes

```csharp
using CatDb.Database.Indexing;
using CatDb.Extensions;

users.CreateIndex("Email", x => x.Email, IndexType.Unique);
users.CreateIndex("City", x => x.City, IndexType.NonUnique);

var byEmail = users.Query(x => x.Email).Equals("ada@example.com").FirstOrDefault();
var london = users.Query(x => x.City).Equals("London").Take(20).ToList();
var londonCount = users.Query(x => x.City).Equals("London").Count();
```

## Build and Validation

```bash
cd src
dotnet build --no-incremental
dotnet test --no-build
dotnet run --project CatDb.GettingStarted
```

Optional stress run:

```bash
cd src/CatDb.StressTest
dotnet run -c Release -- --duration 120
```

## Documentation

- [Documentation site source](docs/README.md)
- [Quick start](docs/docs/quick-start.mdx)
- [Index and search](docs/docs/index-and-search.md)
- [Architecture](docs/docs/architecture.md)
- [API reference](docs/docs/api-reference.md)
- [Troubleshooting](docs/docs/troubleshooting.md)

## Waterfall Tree Reference

Background paper on Waterfall Tree concepts:
https://ieeexplore.ieee.org/document/6857846/references

## License

MIT License — Copyright (c) 2024-2026 CatDb. See [LICENSE](LICENSE) for full text.

Free to use in personal, commercial, and proprietary projects.
