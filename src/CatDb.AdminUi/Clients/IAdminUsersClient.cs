using CatDb.AdminUi.Contracts;

namespace CatDb.AdminUi.Clients;

public interface IAdminUsersClient
{
    Task<PagedResult<UserView>> ListAsync(int page = 1, int pageSize = 20, CancellationToken ct = default);
    Task UpsertAsync(UpsertUserRequest request, CancellationToken ct = default);
    Task DeleteAsync(string userName, CancellationToken ct = default);
}
