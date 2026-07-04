using System.Text.Json;

namespace CatDb.AdminUi.Contracts;

public sealed record KeyValueRow(JsonElement Key, JsonElement Value);

public sealed record BrowseResult(
    string Database, string Table, JsonElement KeySchema, JsonElement ValueSchema,
    string Direction, int Take, int Count, List<KeyValueRow> Rows);

public sealed record QueryResult(
    string Database, string Table, JsonElement KeySchema, JsonElement ValueSchema,
    int Skip, int Take, int Count, long? Total, List<KeyValueRow> Rows);

/// <summary>
/// One "field op value[,value2]" predicate, in the UI's own (pre-JSON) representation.
/// <paramref name="JsonType"/> is the field's JSON Schema type ("string"/"integer"/"number"/"boolean"),
/// used to coerce <paramref name="Value"/>/<paramref name="Value2"/> into the right JSON scalar kind.
/// </summary>
public sealed record FilterCondition(string Field, string Op, string JsonType, string? Value, string? Value2 = null);

public sealed record SortSpec(string Field, bool Desc);

/// <summary>
/// Everything the filter/sort UI needs to express one call to <c>POST .../query</c>.
/// AND-conditions are always applied; OR-conditions (if any) form a single OR-group
/// ANDed with the rest — the same shape the query-string grammar's <c>or=(...)</c> supports.
/// </summary>
public sealed record DataQueryRequest
{
    public List<FilterCondition> AndConditions { get; init; } = [];
    public List<FilterCondition> OrConditions { get; init; } = [];
    public List<SortSpec> Order { get; init; } = [];
    public int? Skip { get; init; }
    public int? Take { get; init; }
    public bool Count { get; init; }
    public string? KeyFrom { get; init; }
    public string? KeyTo { get; init; }
    public string KeyJsonType { get; init; } = "string";
    public bool KeyFromInclusive { get; init; } = true;
    public bool KeyToInclusive { get; init; } = true;
}

/// <summary>Filter operators the query grammar accepts (QueryOps.Parse on the server).</summary>
public static class FilterOps
{
    public const string Equal = "eq";
    public const string GreaterOrEqual = "gte";
    public const string Greater = "gt";
    public const string LessOrEqual = "lte";
    public const string Less = "lt";
    public const string Between = "between";
    public const string Prefix = "prefix";

    public static readonly IReadOnlyList<string> All = [Equal, GreaterOrEqual, Greater, LessOrEqual, Less, Between, Prefix];
}
