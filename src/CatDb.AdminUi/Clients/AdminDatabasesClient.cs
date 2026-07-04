using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;

namespace CatDb.AdminUi.Clients;

/// <summary>Typed client for <c>/api/v1/admin/databases</c> (see AdminDatabaseEndpoints.cs on the server).</summary>
public sealed class AdminDatabasesClient(HttpClient http, ApiCredentialProvider credentials, ApiConnectionState connection)
    : ApiClientBase(http, credentials, connection), IAdminDatabasesClient
{
    public Task<PagedResult<DatabaseRecord>> ListAsync(int page = 1, int pageSize = 20, CancellationToken ct = default)
    {
        var uri = $"/api/v1/admin/databases/?page={page}&pageSize={pageSize}";
        return SendAsync<PagedResult<DatabaseRecord>>(CreateRequest(HttpMethod.Get, uri), ct);
    }

    public Task CreateAsync(string databaseName, CancellationToken ct = default)
    {
        var uri = $"/api/v1/admin/databases/{Uri.EscapeDataString(databaseName)}";
        return SendAsync(CreateRequest(HttpMethod.Post, uri), ct);
    }

    public Task DeleteAsync(string databaseName, CancellationToken ct = default)
    {
        var uri = $"/api/v1/admin/databases/{Uri.EscapeDataString(databaseName)}";
        return SendAsync(CreateRequest(HttpMethod.Delete, uri), ct);
    }
}
