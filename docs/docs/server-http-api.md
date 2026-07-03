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
| `GET` | `/api/v1/data/{databaseName}/{tableName}` | database `Read` | Browse **or query** table rows. |
| `POST` | `/api/v1/data/{databaseName}/{tableName}/query` | database `Read` | Structured query with a nested JSON filter tree. |
| `POST` | `/api/v1/data/{databaseName}/{tableName}` | database `Write` | Insert (InsertOrIgnore). |
| `PUT` | `/api/v1/data/{databaseName}/{tableName}` | database `Write` | Replace (upsert). |
| `DELETE` | `/api/v1/data/{databaseName}/{tableName}` | database `Write` | Delete by key. |

### Browse (fast key scan)

With no filter/order/count params the `GET` endpoint keeps its cheap forward/backward key-scan path:

| Query parameter | Default | Meaning |
| --- | --- | --- |
| `take` / `limit` | `50` | Number of rows to return (max `1000`). |
| `fromKey` | empty | Optional lower key/cursor. |
| `toKey` | empty | Optional upper key. |
| `direction` | `forward` | `forward` or `backward`. |

```bash
curl -u admin:admin \
  "http://localhost:5100/api/v1/data/mydb/events?take=25&direction=forward"
```

### Query (filter, order, paging, count) — all optional

Add field predicates / `order` / `count` to the **same** `GET` endpoint and it runs the engine's query
planner (index seeks, multi-index AND/OR intersection, engine-side residual, and ORDER BY) — filtering and
sorting happen **inside the engine**, never by materializing the whole table. Grammar (PostgREST-inspired,
colon-delimited so values may contain dots):

| Form | Meaning |
| --- | --- |
| `Field=value` | `Field = value` (bare value ⇒ equality) |
| `Field=eq:value` | `Field = value` |
| `Field=gt:v` / `gte:v` / `lt:v` / `lte:v` | `>`, `>=`, `<`, `<=` |
| `Field=between:lo:hi` | `lo <= Field <= hi` |
| `Field=prefix:p` | string/bytes field starts with `p` |
| repeat a field | ANDed (`?Age=gte:30&Age=lt:65`) |
| `or=(A:eq:1,B:eq:2)` | one OR group, ANDed with the other predicates |
| `order=Field:desc,Other` | ORDER BY (`-Field` = desc shorthand; `$key` = primary key) |
| `limit` / `offset` (or `take` / `skip`) | paging |
| `count=true` | include the total match count (fast — no per-row fetch) |
| `fromKey` / `toKey` | primary-key range, composes with the filters |

```bash
# City = 'nyc' AND Age >= 30, newest first, first 20, with total count
curl -u admin:admin \
  "http://localhost:5100/api/v1/data/mydb/people?City=nyc&Age=gte:30&order=Age:desc&limit=20&count=true"

# (City = 'nyc' OR City = 'la') AND Age between 30 and 65
curl -u admin:admin \
  "http://localhost:5100/api/v1/data/mydb/people?or=(City:eq:nyc,City:eq:la)&Age=between:30:65"
```

For arbitrarily deep / chained boolean logic (AND/OR/NOT nesting with no expressiveness limit), `POST` the
same query as a JSON tree to `…/{table}/query`. A filter node is either a predicate (`{field, op, value[,
value2]}`) or a combinator (`{and|or: [nodes]}` / `{not: node}`):

```bash
curl -u admin:admin -X POST \
  "http://localhost:5100/api/v1/data/mydb/people/query" \
  -H "Content-Type: application/json" -d '{
    "filter": { "and": [
      { "field": "City", "op": "eq", "value": "nyc" },
      { "or": [ { "field": "Age", "op": "lt", "value": 10 },
                { "field": "Age", "op": "gt", "value": 90 } ] } ] },
    "order": [ { "field": "Age", "desc": true }, { "field": "$key" } ],
    "skip": 0, "take": 20, "count": true }'
```

Both forms return the same shape: `{ database, table, keySchema, valueSchema, skip, take, count, total, rows }`
(`total` is present only when `count` was requested; `count` is the number of rows in this page). Field
predicates require an **object (Slots) record** — index/sort a scalar field by its member name. Sorting by a
field that has a matching secondary index streams in index order; otherwise the engine buffers and sorts the
page. An unknown field or operator returns `400`.

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
