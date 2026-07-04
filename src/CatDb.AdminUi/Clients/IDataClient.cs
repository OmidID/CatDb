using System.Text.Json.Nodes;
using CatDb.AdminUi.Contracts;

namespace CatDb.AdminUi.Clients;

public interface IDataClient
{
    Task<BrowseResult> BrowseAsync(
        string database, string table, int take, string? fromKey, string? toKey, string direction,
        CancellationToken ct = default);

    Task<QueryResult> QueryAsync(string database, string table, DataQueryRequest request, CancellationToken ct = default);

    Task InsertAsync(string database, string table, JsonNode key, JsonNode value, CancellationToken ct = default);
    Task ReplaceAsync(string database, string table, JsonNode key, JsonNode value, CancellationToken ct = default);
    Task DeleteRecordAsync(string database, string table, JsonNode key, CancellationToken ct = default);
}
