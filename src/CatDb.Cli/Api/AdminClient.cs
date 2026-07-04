// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Cli.Session;

namespace CatDb.Cli.Api;

/// <summary>Client for the <c>/api/v1/admin/databases</c> and <c>/api/v1/admin/users</c> endpoints
/// (<c>AdminDatabaseEndpoints</c>/<c>AdminUserEndpoints</c> on CatDb.Server).</summary>
public interface IAdminClient
{
    Task<PagedResult<DatabaseRecord>> ListDatabasesAsync(int page, int pageSize, CancellationToken ct = default);
    Task CreateDatabaseAsync(string name, CancellationToken ct = default);
    Task DeleteDatabaseAsync(string name, CancellationToken ct = default);

    Task<PagedResult<UserView>> ListUsersAsync(int page, int pageSize, CancellationToken ct = default);
    Task UpsertUserAsync(UpsertUserRequest request, CancellationToken ct = default);
    Task DeleteUserAsync(string userName, CancellationToken ct = default);
}

public sealed class AdminClient(HttpClient http, CliSession session) : ApiClientBase(http, session), IAdminClient
{
    public Task<PagedResult<DatabaseRecord>> ListDatabasesAsync(int page, int pageSize, CancellationToken ct = default) =>
        SendAsync<PagedResult<DatabaseRecord>>(
            CreateRequest(HttpMethod.Get, $"api/v1/admin/databases/?page={page}&pageSize={pageSize}"), ct);

    public Task CreateDatabaseAsync(string name, CancellationToken ct = default) =>
        SendAsync(CreateRequest(HttpMethod.Post, $"api/v1/admin/databases/{Uri.EscapeDataString(name)}"), ct);

    public Task DeleteDatabaseAsync(string name, CancellationToken ct = default) =>
        SendAsync(CreateRequest(HttpMethod.Delete, $"api/v1/admin/databases/{Uri.EscapeDataString(name)}"), ct);

    public Task<PagedResult<UserView>> ListUsersAsync(int page, int pageSize, CancellationToken ct = default) =>
        SendAsync<PagedResult<UserView>>(
            CreateRequest(HttpMethod.Get, $"api/v1/admin/users/?page={page}&pageSize={pageSize}"), ct);

    public Task UpsertUserAsync(UpsertUserRequest request, CancellationToken ct = default) =>
        SendAsync(CreateRequest(HttpMethod.Post, "api/v1/admin/users/", request), ct);

    public Task DeleteUserAsync(string userName, CancellationToken ct = default) =>
        SendAsync(CreateRequest(HttpMethod.Delete, $"api/v1/admin/users/{Uri.EscapeDataString(userName)}"), ct);
}
