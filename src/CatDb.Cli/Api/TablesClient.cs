// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Cli.Session;

namespace CatDb.Cli.Api;

/// <summary>Client for the <c>/api/v1/tables/{db}</c> table+index management endpoints
/// (<c>DataTableManagementEndpoints</c> on CatDb.Server).</summary>
public interface ITablesClient
{
    Task<TableListResponse> ListTablesAsync(string database, CancellationToken ct = default);
    Task<TableInfo> CreateTableAsync(string database, CreateTableRequest request, CancellationToken ct = default);
    Task<TableInfo> GetTableAsync(string database, string table, CancellationToken ct = default);
    Task DeleteTableAsync(string database, string table, CancellationToken ct = default);

    Task<List<IndexInfo>> ListIndexesAsync(string database, string table, CancellationToken ct = default);
    Task<IndexInfo> CreateIndexAsync(string database, string table, CreateIndexRequest request, CancellationToken ct = default);
    Task DropIndexAsync(string database, string table, string indexName, CancellationToken ct = default);
    Task RebuildIndexAsync(string database, string table, string? indexName, CancellationToken ct = default);
}

public sealed class TablesClient(HttpClient http, CliSession session) : ApiClientBase(http, session), ITablesClient
{
    public Task<TableListResponse> ListTablesAsync(string database, CancellationToken ct = default) =>
        SendAsync<TableListResponse>(CreateRequest(HttpMethod.Get, $"api/v1/tables/{Enc(database)}/"), ct);

    public Task<TableInfo> CreateTableAsync(string database, CreateTableRequest request, CancellationToken ct = default) =>
        SendAsync<TableInfo>(CreateRequest(HttpMethod.Post, $"api/v1/tables/{Enc(database)}/", request), ct);

    public Task<TableInfo> GetTableAsync(string database, string table, CancellationToken ct = default) =>
        SendAsync<TableInfo>(CreateRequest(HttpMethod.Get, $"api/v1/tables/{Enc(database)}/{Enc(table)}"), ct);

    public Task DeleteTableAsync(string database, string table, CancellationToken ct = default) =>
        SendAsync(CreateRequest(HttpMethod.Delete, $"api/v1/tables/{Enc(database)}/{Enc(table)}"), ct);

    public async Task<List<IndexInfo>> ListIndexesAsync(string database, string table, CancellationToken ct = default)
    {
        var response = await SendAsync<IndexListEnvelope>(
            CreateRequest(HttpMethod.Get, $"api/v1/tables/{Enc(database)}/{Enc(table)}/indexes"), ct).ConfigureAwait(false);
        return response.Indexes;
    }

    public Task<IndexInfo> CreateIndexAsync(string database, string table, CreateIndexRequest request, CancellationToken ct = default) =>
        SendAsync<IndexInfo>(CreateRequest(HttpMethod.Post, $"api/v1/tables/{Enc(database)}/{Enc(table)}/indexes", request), ct);

    public Task DropIndexAsync(string database, string table, string indexName, CancellationToken ct = default) =>
        SendAsync(CreateRequest(HttpMethod.Delete, $"api/v1/tables/{Enc(database)}/{Enc(table)}/indexes/{Enc(indexName)}"), ct);

    public Task RebuildIndexAsync(string database, string table, string? indexName, CancellationToken ct = default) =>
        SendAsync(CreateRequest(HttpMethod.Post, indexName is null
            ? $"api/v1/tables/{Enc(database)}/{Enc(table)}/indexes/rebuild"
            : $"api/v1/tables/{Enc(database)}/{Enc(table)}/indexes/{Enc(indexName)}/rebuild"), ct);

    private static string Enc(string value) => Uri.EscapeDataString(value);

    private sealed record IndexListEnvelope(string Database, string Table, List<IndexInfo> Indexes);
}
