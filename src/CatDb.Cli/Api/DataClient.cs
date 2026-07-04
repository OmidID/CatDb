// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Text.Json;
using CatDb.Cli.Session;

namespace CatDb.Cli.Api;

/// <summary>Client for the <c>/api/v1/data/{db}/{table}</c> row endpoints
/// (<c>DataTableEndpoints</c> on CatDb.Server).</summary>
public interface IDataClient
{
    /// <summary>Cheap key-range browse — no query grammar involved.</summary>
    Task<BrowseResult> BrowseAsync(string database, string table, int take, string? fromKey, string? toKey,
        string direction, CancellationToken ct = default);

    /// <summary>Structured query — <paramref name="queryString"/> is the already-encoded field
    /// predicate/order/paging grammar described in QueryModels.cs (e.g. "City=nyc&amp;order=Age:desc&amp;limit=20").</summary>
    Task<QueryResult> QueryAsync(string database, string table, string queryString, CancellationToken ct = default);

    Task<MutationResult> InsertAsync(string database, string table, JsonElement key, JsonElement value, CancellationToken ct = default);
    Task<MutationResult> ReplaceAsync(string database, string table, JsonElement key, JsonElement value, CancellationToken ct = default);
    Task DeleteAsync(string database, string table, JsonElement key, CancellationToken ct = default);
}

public sealed class DataClient(HttpClient http, CliSession session) : ApiClientBase(http, session), IDataClient
{
    public Task<BrowseResult> BrowseAsync(string database, string table, int take, string? fromKey, string? toKey,
        string direction, CancellationToken ct = default)
    {
        var qs = $"take={take}&direction={Uri.EscapeDataString(direction)}";
        if (!string.IsNullOrEmpty(fromKey)) qs += $"&fromKey={Uri.EscapeDataString(fromKey)}";
        if (!string.IsNullOrEmpty(toKey)) qs += $"&toKey={Uri.EscapeDataString(toKey)}";

        return SendAsync<BrowseResult>(CreateRequest(HttpMethod.Get, $"api/v1/data/{Enc(database)}/{Enc(table)}?{qs}"), ct);
    }

    public Task<QueryResult> QueryAsync(string database, string table, string queryString, CancellationToken ct = default) =>
        SendAsync<QueryResult>(CreateRequest(HttpMethod.Get, $"api/v1/data/{Enc(database)}/{Enc(table)}?{queryString}"), ct);

    public Task<MutationResult> InsertAsync(string database, string table, JsonElement key, JsonElement value, CancellationToken ct = default) =>
        SendAsync<MutationResult>(CreateRequest(HttpMethod.Post, $"api/v1/data/{Enc(database)}/{Enc(table)}",
            new { key, value }), ct);

    public Task<MutationResult> ReplaceAsync(string database, string table, JsonElement key, JsonElement value, CancellationToken ct = default) =>
        SendAsync<MutationResult>(CreateRequest(HttpMethod.Put, $"api/v1/data/{Enc(database)}/{Enc(table)}",
            new { key, value }), ct);

    public Task DeleteAsync(string database, string table, JsonElement key, CancellationToken ct = default) =>
        SendAsync(CreateRequest(HttpMethod.Delete, $"api/v1/data/{Enc(database)}/{Enc(table)}", new { key }), ct);

    private static string Enc(string value) => Uri.EscapeDataString(value);
}
