using CatDb.AdminUi.Contracts;

namespace CatDb.AdminUi.Clients;

public interface IAdminDatabasesClient
{
    Task<PagedResult<DatabaseRecord>> ListAsync(int page = 1, int pageSize = 20, CancellationToken ct = default);
    Task CreateAsync(string databaseName, CancellationToken ct = default);
    Task DeleteAsync(string databaseName, CancellationToken ct = default);
}
