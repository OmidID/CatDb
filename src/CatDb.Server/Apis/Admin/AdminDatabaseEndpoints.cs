using CatDb.Server.Services;

namespace CatDb.Server.Apis.Admin;

public static class AdminDatabaseEndpoints
{
    public static IEndpointRouteBuilder MapAdminDatabaseEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/v1/admin/databases");

        group.MapGet("/", (
            HttpContext http,
            SystemCatalogService catalog,
            DatabaseHostService host,
            int page = 1,
            int pageSize = 20) =>
        {
            var authResult = Authorize(http, catalog, GlobalPermission.ListDatabases);
            if (authResult.Result != null)
                return authResult.Result;

            page = Math.Max(1, page);
            pageSize = Math.Clamp(pageSize, 1, 200);

            var (items, total) = host.ListDatabases(page, pageSize);
            return Results.Ok(new
            {
                page,
                pageSize,
                total,
                items,
            });
        });

        group.MapPost("/{databaseName}", (
            HttpContext http,
            SystemCatalogService catalog,
            DatabaseHostService host,
            string databaseName) =>
        {
            var authResult = Authorize(http, catalog, GlobalPermission.ManageDatabases);
            if (authResult.Result != null)
                return authResult.Result;

            if (string.IsNullOrWhiteSpace(databaseName))
                return Results.BadRequest(new { error = "databaseName is required." });

            try
            {
                host.CreateDatabase(databaseName);
                return Results.Ok(new { name = databaseName, created = true });
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        group.MapDelete("/{databaseName}", (
            HttpContext http,
            SystemCatalogService catalog,
            DatabaseHostService host,
            string databaseName) =>
        {
            var authResult = Authorize(http, catalog, GlobalPermission.ManageDatabases);
            if (authResult.Result != null)
                return authResult.Result;

            if (string.IsNullOrWhiteSpace(databaseName))
                return Results.BadRequest(new { error = "databaseName is required." });

            try
            {
                host.DeleteDatabase(databaseName);
                return Results.Ok(new { name = databaseName, deleted = true });
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        return app;
    }

    private static (IResult? Result, AuthenticatedUser? User) Authorize(
        HttpContext http,
        SystemCatalogService catalog,
        GlobalPermission requiredPermission)
    {
        if (!BasicAuthHelpers.TryReadCredentials(http, out var userName, out var password))
            return (Results.Unauthorized(), null);

        var user = catalog.Authenticate(userName, password);
        if (user == null)
            return (Results.Unauthorized(), null);

        if (!user.HasGlobal(requiredPermission))
            return (Results.StatusCode(StatusCodes.Status403Forbidden), null);

        return (null, user);
    }
}
