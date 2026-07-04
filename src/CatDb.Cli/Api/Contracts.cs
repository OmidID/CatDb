// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Text.Json;

namespace CatDb.Cli.Api;

// Wire DTOs for CatDb.Server's HTTP API (src/CatDb.Server/Apis, Models). Kept independent from
// CatDb.Server/CatDb.AdminUi (no project reference) so the CLI ships as a small, standalone tool
// that only depends on the HTTP contract, not the server's implementation types.

public sealed record PagedResult<T>(int Page, int PageSize, int Total, List<T> Items);

public sealed record DatabaseRecord(string Name, DateTime CreatedAtUtc);

public sealed record UserView(string UserName, string GlobalPermissions, string DatabasePermissions);

public sealed record UpsertUserRequest(
    string UserName,
    string Password,
    string GlobalPermissions,
    Dictionary<string, string> DatabasePermissions);

public sealed record IndexInfo(string Name, List<string> Members, List<int> SlotIndices, string Type);

public sealed record TableSummary(string Name, JsonElement KeySchema, JsonElement ValueSchema, DateTime CreatedAt, DateTime ModifiedAt);

public sealed record TableInfo(
    string Database,
    string Name,
    JsonElement KeySchema,
    JsonElement ValueSchema,
    DateTime CreatedAt,
    DateTime ModifiedAt,
    List<IndexInfo> Indexes);

public sealed record TableListResponse(string Database, int Count, List<TableSummary> Tables);

public sealed record CreateTableRequest(string Name, JsonElement KeySchema, JsonElement ValueSchema);

public sealed record CreateIndexRequest(string IndexName, List<string> Members, string Type = "NonUnique");

public sealed record KeyValueRow(JsonElement Key, JsonElement Value);

/// <summary>Shape returned by the row insert/replace/delete endpoints (DataExplorerService.InsertRecord/
/// ReplaceRecord/DeleteRecord return <c>{ success, operation }</c>, not the row itself).</summary>
public sealed record MutationResult(bool Success, string Operation);

public sealed record BrowseResult(
    string Database, string Table, JsonElement KeySchema, JsonElement ValueSchema,
    string Direction, int Take, int Count, List<KeyValueRow> Rows);

public sealed record QueryResult(
    string Database, string Table, JsonElement KeySchema, JsonElement ValueSchema,
    int Skip, int Take, int Count, long? Total, List<KeyValueRow> Rows);

/// <summary>Index "Type" values accepted by <see cref="CreateIndexRequest"/>.</summary>
public static class IndexTypes
{
    public const string Unique = "Unique";
    public const string NonUnique = "NonUnique";
}
