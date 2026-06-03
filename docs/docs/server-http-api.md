---
id: server-http-api
title: Server HTTP API
---

# Server HTTP API

`CatDb.Server` exposes a compact HTTP API for administration, health, and read-only data exploration. All protected endpoints use Basic authentication.

Default base URL:

```text
http://localhost:5100
```

## Health

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/` | Service name and assembly version. |
| `GET` | `/health` | All health checks. |
| `GET` | `/health/catdb` | CatDb health check only. |

## Databases

| Method | Path | Permission | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/admin/databases?page=1&pageSize=20` | `ListDatabases` | List registered databases. |
| `POST` | `/api/v1/admin/databases/{databaseName}` | `ManageDatabases` | Create/register a database. |
| `DELETE` | `/api/v1/admin/databases/{databaseName}` | `ManageDatabases` | Delete a database. |

Example:

```bash
curl -u admin:admin http://localhost:5100/api/v1/admin/databases
```

## Users

| Method | Path | Permission | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/admin/users?page=1&pageSize=20` | `ManageUsers` | List users. |
| `POST` | `/api/v1/admin/users` | `ManageUsers` | Create or update a user. |
| `DELETE` | `/api/v1/admin/users/{userName}` | `ManageUsers` | Delete a user. |

Request body for `POST /api/v1/admin/users`:

```json
{
  "userName": "reader",
  "password": "changeme",
  "globalPermissions": "ListDatabases",
  "databasePermissions": {
    "mydb": "Read"
  }
}
```

## Data explorer

| Method | Path | Permission | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/data/{databaseName}` | database `Read` | List XTABLE descriptors in a database. |
| `GET` | `/api/v1/data/{databaseName}/{tableName}` | database `Read` | Browse table rows. |

Browse parameters:

| Query parameter | Default | Meaning |
| --- | --- | --- |
| `take` | `50` | Number of rows to return. |
| `fromKey` | empty | Optional lower cursor/key. |
| `toKey` | empty | Optional upper key. |
| `direction` | `forward` | `forward` or `backward`. |

Example:

```bash
curl -u admin:admin \
  "http://localhost:5100/api/v1/data/mydb/events?take=25&direction=forward"
```

## Permissions

Global permissions:

- `None`
- `ListDatabases`
- `ManageDatabases`
- `ManageUsers`
- `Admin`

Database permissions:

- `None`
- `Read`
- `Write`
- `TableAdmin`
- `HeapAccess`
- `Admin`

Flags can be combined using the standard enum text form, for example `"Read, Write"`.
