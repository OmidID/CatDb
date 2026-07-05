---
id: setup-server
title: Setup Server
---

# Setup server

`CatDb.Server` hosts CatDb databases from a directory and exposes health, administration, and data browsing endpoints over HTTP. The core library also has a TCP client/server path via `CatDb.Database.CatDb.CreateServer(...)` and `FromNetwork(...)`.

## Run the ASP.NET Core server

From the repository:

```bash
cd src
dotnet run --project CatDb.Server
```

Default settings in `CatDb.Server/appsettings.json`:

```json
{
  "CatDb": {
    "Directory": "database",
    "Port": 7182
  },
  "Urls": "http://localhost:5100"
}
```

You can override the database directory, default TCP port, and default database from the command line:

```bash
dotnet run --project CatDb.Server -- --catdb-dir ./database --catdb-port 7182 --catdb-default-db default
```

## First login

On first startup, the server initializes `system.catdb`. If no users exist, it bootstraps a default administrator:

| Field | Value |
| --- | --- |
| User name | `admin` |
| Password | `admin` |
| Permissions | global admin plus wildcard database admin |

Change this user before exposing the server outside a local development machine.

## Health checks

```bash
curl http://localhost:5100/
curl http://localhost:5100/health
curl http://localhost:5100/health/catdb
```

## Admin endpoints

All admin endpoints use Basic authentication.

```bash
curl -u admin:admin http://localhost:5100/api/v1/admin/databases

curl -u admin:admin -X POST \
  http://localhost:5100/api/v1/admin/databases/mydb

curl -u admin:admin -X DELETE \
  http://localhost:5100/api/v1/admin/databases/mydb
```

Create or update a user:

```bash
curl -u admin:admin -X POST \
  http://localhost:5100/api/v1/admin/users \
  -H "Content-Type: application/json" \
  -d '{
    "userName": "reader",
    "password": "changeme",
    "globalPermissions": "ListDatabases",
    "databasePermissions": {
      "mydb": "Read"
    }
  }'
```

## Data browsing endpoints

List tables in a database:

```bash
curl -u admin:admin http://localhost:5100/api/v1/data/mydb
```

Browse table rows:

```bash
curl -u admin:admin \
  "http://localhost:5100/api/v1/data/mydb/ticks?take=50&direction=forward&fromKey=&toKey="
```

The repository includes a Bruno collection under `bruno/` with ready-to-run requests for health, admin databases, admin users, and data browsing.

## TCP client

Client code can connect to the storage engine API through TCP:

```csharp
using var engine = CatDb.Database.CatDb.FromNetwork(
    host: "localhost",
    port: 7182,
    databaseName: "mydb",
    userName: "admin",
    password: "admin");

var table = engine.OpenXTable<long, string>("events");
table[1] = "from client";
engine.Commit();
```

Or the same connection via a single connection string (see [Database engine](database-engine.md#connection-strings) for every key/alias):

```csharp
using var engine = CatDb.Database.CatDb.FromConnectionString(
    "Provider=Network;Host=localhost;Port=7182;Database=mydb;User Id=admin;Password=admin");
```
