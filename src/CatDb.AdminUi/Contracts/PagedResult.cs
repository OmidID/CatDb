namespace CatDb.AdminUi.Contracts;

public sealed record PagedResult<T>(int Page, int PageSize, int Total, List<T> Items);
