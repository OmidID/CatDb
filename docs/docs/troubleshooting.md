---
id: troubleshooting
title: Troubleshooting
---

# Troubleshooting

## My data disappeared after restart

Call `engine.Commit()` before closing the process. `Close()` and `Dispose()` do not commit pending writes.

## A table throws when reopened

The table name already exists with a different schema or structure type. CatDb validates key type, record type, and whether the structure is an `XTABLE` or `XFILE`.

## Server rejects requests

Check Basic authentication and permissions. A new system catalog creates `admin` / `admin` only when there are no users. After that, users are read from `system.catdb`.

## HTTP server starts but TCP clients cannot connect

The HTTP URL is configured through `Urls`, defaulting to `http://localhost:5100`. The CatDb TCP port is configured separately through `CatDb:Port`, defaulting to `7182`.

## Range pages become slow at high page numbers

Offset paging must scan skipped rows. Prefer keyset paging with `PageAfter(query, afterKey, take)`.

## Running the stress test

Use a duration when running from automation:

```bash
cd src/CatDb.StressTest
dotnet run -c Release -- --duration 120
```

The stress test writes `stress_errors.log` and `catdb_stress.db` in its working directory.

## Useful local commands

```bash
cd src
dotnet build --no-incremental
dotnet test --no-build
dotnet run --project CatDb.GettingStarted
```
