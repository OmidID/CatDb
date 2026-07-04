using CatDb.AdminUi.Contracts;

namespace CatDb.AdminUi.Clients;

public interface ITablesClient
{
    Task<List<TableSummary>> ListAsync(string database, CancellationToken ct = default);
    Task<TableInfo> GetAsync(string database, string table, CancellationToken ct = default);
    Task<TableInfo> CreateAsync(string database, CreateTableRequest request, CancellationToken ct = default);
    Task DeleteAsync(string database, string table, CancellationToken ct = default);

    Task<List<IndexInfo>> ListIndexesAsync(string database, string table, CancellationToken ct = default);
    Task<IndexInfo> CreateIndexAsync(string database, string table, CreateIndexRequest request, CancellationToken ct = default);
    Task DeleteIndexAsync(string database, string table, string indexName, CancellationToken ct = default);
    Task RebuildIndexAsync(string database, string table, string indexName, CancellationToken ct = default);
    Task RebuildAllIndexesAsync(string database, string table, CancellationToken ct = default);
}
