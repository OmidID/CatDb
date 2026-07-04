using System.Text.Json;

namespace CatDb.AdminUi.Contracts;

public sealed record TableSummary(string Name, JsonElement KeySchema, JsonElement ValueSchema, DateTime CreatedAt, DateTime ModifiedAt);

public sealed record IndexInfo(string Name, List<string> Members, List<int> SlotIndices, string Type);

public sealed record TableInfo(
    string Database,
    string Name,
    JsonElement KeySchema,
    JsonElement ValueSchema,
    DateTime CreatedAt,
    DateTime ModifiedAt,
    List<IndexInfo> Indexes);

public sealed record CreateTableRequest(string Name, JsonElement KeySchema, JsonElement ValueSchema);

public sealed record CreateIndexRequest(string IndexName, List<string> Members, string Type = "NonUnique");

public sealed record TableListResponse(string Database, int Count, List<TableSummary> Tables);

public sealed record IndexListResponse(string Database, string Table, List<IndexInfo> Indexes);

/// <summary>Index "Type" values accepted by <see cref="CreateIndexRequest"/>.</summary>
public static class IndexTypes
{
    public const string Unique = "Unique";
    public const string NonUnique = "NonUnique";

    public static readonly IReadOnlyList<string> All = [NonUnique, Unique];
}
