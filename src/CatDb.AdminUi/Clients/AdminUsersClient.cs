using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;

namespace CatDb.AdminUi.Clients;

/// <summary>Typed client for <c>/api/v1/admin/users</c> (see AdminUserEndpoints.cs on the server).</summary>
public sealed class AdminUsersClient(HttpClient http, ApiCredentialProvider credentials, ApiConnectionState connection)
    : ApiClientBase(http, credentials, connection), IAdminUsersClient
{
    public Task<PagedResult<UserView>> ListAsync(int page = 1, int pageSize = 20, CancellationToken ct = default)
    {
        var uri = $"/api/v1/admin/users/?page={page}&pageSize={pageSize}";
        return SendAsync<PagedResult<UserView>>(CreateRequest(HttpMethod.Get, uri), ct);
    }

    public Task UpsertAsync(UpsertUserRequest request, CancellationToken ct = default) =>
        SendAsync(CreateRequest(HttpMethod.Post, "/api/v1/admin/users/", request), ct);

    public Task DeleteAsync(string userName, CancellationToken ct = default)
    {
        var uri = $"/api/v1/admin/users/{Uri.EscapeDataString(userName)}";
        return SendAsync(CreateRequest(HttpMethod.Delete, uri), ct);
    }
}
