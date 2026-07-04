using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;

namespace CatDb.AdminUi.Clients;

/// <summary>Typed client for <c>/api/v1/tables/{db}</c> (see DataTableManagementEndpoints.cs on the server).</summary>
public sealed class TablesClient(HttpClient http, ApiCredentialProvider credentials, ApiConnectionState connection)
    : ApiClientBase(http, credentials, connection), ITablesClient
{
    public async Task<List<TableSummary>> ListAsync(string database, CancellationToken ct = default)
    {
        var uri = $"/api/v1/tables/{Uri.EscapeDataString(database)}/";
        var response = await SendAsync<TableListResponse>(CreateRequest(HttpMethod.Get, uri), ct).ConfigureAwait(false);
        return response.Tables;
    }

    public Task<TableInfo> GetAsync(string database, string table, CancellationToken ct = default)
    {
        var uri = $"/api/v1/tables/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}";
        return SendAsync<TableInfo>(CreateRequest(HttpMethod.Get, uri), ct);
    }

    public Task<TableInfo> CreateAsync(string database, CreateTableRequest request, CancellationToken ct = default)
    {
        var uri = $"/api/v1/tables/{Uri.EscapeDataString(database)}/";
        return SendAsync<TableInfo>(CreateRequest(HttpMethod.Post, uri, request), ct);
    }

    public Task DeleteAsync(string database, string table, CancellationToken ct = default)
    {
        var uri = $"/api/v1/tables/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}";
        return SendAsync(CreateRequest(HttpMethod.Delete, uri), ct);
    }

    public async Task<List<IndexInfo>> ListIndexesAsync(string database, string table, CancellationToken ct = default)
    {
        var uri = $"/api/v1/tables/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}/indexes";
        var response = await SendAsync<IndexListResponse>(CreateRequest(HttpMethod.Get, uri), ct).ConfigureAwait(false);
        return response.Indexes;
    }

    public Task<IndexInfo> CreateIndexAsync(string database, string table, CreateIndexRequest request, CancellationToken ct = default)
    {
        var uri = $"/api/v1/tables/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}/indexes";
        return SendAsync<IndexInfo>(CreateRequest(HttpMethod.Post, uri, request), ct);
    }

    public Task DeleteIndexAsync(string database, string table, string indexName, CancellationToken ct = default)
    {
        var uri = $"/api/v1/tables/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}/indexes/{Uri.EscapeDataString(indexName)}";
        return SendAsync(CreateRequest(HttpMethod.Delete, uri), ct);
    }

    public Task RebuildIndexAsync(string database, string table, string indexName, CancellationToken ct = default)
    {
        var uri = $"/api/v1/tables/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}/indexes/{Uri.EscapeDataString(indexName)}/rebuild";
        return SendAsync(CreateRequest(HttpMethod.Post, uri), ct);
    }

    public Task RebuildAllIndexesAsync(string database, string table, CancellationToken ct = default)
    {
        var uri = $"/api/v1/tables/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}/indexes/rebuild";
        return SendAsync(CreateRequest(HttpMethod.Post, uri), ct);
    }
}
