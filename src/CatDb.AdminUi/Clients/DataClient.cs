using System.Text.Json.Nodes;
using System.Web;
using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;

namespace CatDb.AdminUi.Clients;

/// <summary>Typed client for <c>/api/v1/data/{db}/{table}</c> (see DataTableEndpoints.cs on the server).</summary>
public sealed class DataClient(HttpClient http, ApiCredentialProvider credentials, ApiConnectionState connection)
    : ApiClientBase(http, credentials, connection), IDataClient
{
    public Task<BrowseResult> BrowseAsync(
        string database, string table, int take, string? fromKey, string? toKey, string direction,
        CancellationToken ct = default)
    {
        var query = HttpUtility.ParseQueryString(string.Empty);
        query["take"] = take.ToString();
        query["direction"] = direction;
        if (!string.IsNullOrEmpty(fromKey)) query["fromKey"] = fromKey;
        if (!string.IsNullOrEmpty(toKey)) query["toKey"] = toKey;

        var uri = $"/api/v1/data/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}?{query}";
        return SendAsync<BrowseResult>(CreateRequest(HttpMethod.Get, uri), ct);
    }

    public Task<QueryResult> QueryAsync(string database, string table, DataQueryRequest request, CancellationToken ct = default)
    {
        var uri = $"/api/v1/data/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}/query";
        var body = BuildQueryBody(request);
        return SendAsync<QueryResult>(CreateRequest(HttpMethod.Post, uri, body), ct);
    }

    public Task InsertAsync(string database, string table, JsonNode key, JsonNode value, CancellationToken ct = default)
    {
        var uri = $"/api/v1/data/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}";
        var body = new JsonObject { ["key"] = key, ["value"] = value };
        return SendAsync(CreateRequest(HttpMethod.Post, uri, body), ct);
    }

    public Task ReplaceAsync(string database, string table, JsonNode key, JsonNode value, CancellationToken ct = default)
    {
        var uri = $"/api/v1/data/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}";
        var body = new JsonObject { ["key"] = key, ["value"] = value };
        return SendAsync(CreateRequest(HttpMethod.Put, uri, body), ct);
    }

    public Task DeleteRecordAsync(string database, string table, JsonNode key, CancellationToken ct = default)
    {
        var uri = $"/api/v1/data/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(table)}";
        var body = new JsonObject { ["key"] = key };
        return SendAsync(CreateRequest(HttpMethod.Delete, uri, body), ct);
    }

    // ── Query DTO → JSON body (matches JsonQueryParser.Parse on the server) ────

    private static JsonObject BuildQueryBody(DataQueryRequest request)
    {
        var body = new JsonObject();

        var filter = BuildFilter(request);
        if (filter is not null)
            body["filter"] = filter;

        if (request.Order.Count > 0)
        {
            var order = new JsonArray();
            foreach (var sort in request.Order)
                order.Add(new JsonObject { ["field"] = sort.Field, ["desc"] = sort.Desc });
            body["order"] = order;
        }

        if (request.Skip is { } skip) body["skip"] = skip;
        if (request.Take is { } take) body["take"] = take;
        if (request.Count) body["count"] = true;

        if (request.KeyFrom is not null)
            body["keyFrom"] = JsonScalarCoercion.ToJsonNode(request.KeyJsonType, null, request.KeyFrom);
        if (request.KeyTo is not null)
            body["keyTo"] = JsonScalarCoercion.ToJsonNode(request.KeyJsonType, null, request.KeyTo);
        if (!request.KeyFromInclusive) body["keyFromInclusive"] = false;
        if (!request.KeyToInclusive) body["keyToInclusive"] = false;

        return body;
    }

    private static JsonObject? BuildFilter(DataQueryRequest request)
    {
        var and = new JsonArray();
        foreach (var condition in request.AndConditions)
            and.Add(BuildPredicate(condition));

        if (request.OrConditions.Count > 0)
        {
            var or = new JsonArray();
            foreach (var condition in request.OrConditions)
                or.Add(BuildPredicate(condition));
            and.Add(new JsonObject { ["or"] = or });
        }

        return and.Count == 0 ? null : new JsonObject { ["and"] = and };
    }

    private static JsonObject BuildPredicate(FilterCondition condition)
    {
        var node = new JsonObject { ["field"] = condition.Field, ["op"] = condition.Op };

        if (condition.Value is not null)
            node["value"] = JsonScalarCoercion.ToJsonNode(condition.JsonType, null, condition.Value);

        if (condition.Op == FilterOps.Between && condition.Value2 is not null)
            node["value2"] = JsonScalarCoercion.ToJsonNode(condition.JsonType, null, condition.Value2);

        return node;
    }
}
