using CatDb.Server.Services;

namespace CatDb.Server.Apis.Admin;

public static class AdminUserEndpoints
{
    public static IEndpointRouteBuilder MapAdminUserEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/v1/admin/users");

        group.MapGet("/", (
            HttpContext http,
            SystemCatalogService catalog,
            int page = 1,
            int pageSize = 20) =>
        {
            var authResult = Authorize(http, catalog, GlobalPermission.ManageUsers);
            if (authResult.Result != null)
                return authResult.Result;

            page = Math.Max(1, page);
            pageSize = Math.Clamp(pageSize, 1, 200);

            var (items, total) = catalog.ListUsers(page, pageSize);
            return Results.Ok(new
            {
                page,
                pageSize,
                total,
                items,
            });
        });

        group.MapPost("/", (
            HttpContext http,
            SystemCatalogService catalog,
            UpsertAdminUserRequest request) =>
        {
            var authResult = Authorize(http, catalog, GlobalPermission.ManageUsers);
            if (authResult.Result != null)
                return authResult.Result;

            if (string.IsNullOrWhiteSpace(request.UserName) || string.IsNullOrWhiteSpace(request.Password))
                return Results.BadRequest(new { error = "userName and password are required." });

            if (!Enum.TryParse<GlobalPermission>(request.GlobalPermissions, ignoreCase: true, out var globalPermissions))
                return Results.BadRequest(new { error = "Invalid globalPermissions value." });

            var databasePermissions = new Dictionary<string, DatabasePermission>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in request.DatabasePermissions)
            {
                if (!Enum.TryParse<DatabasePermission>(kv.Value, ignoreCase: true, out var parsed))
                    return Results.BadRequest(new { error = $"Invalid database permission for '{kv.Key}'." });

                databasePermissions[kv.Key] = parsed;
            }

            catalog.UpsertUser(request.UserName, request.Password, globalPermissions, databasePermissions);
            return Results.Ok(new { userName = request.UserName, updated = true });
        });

        group.MapDelete("/{userName}", (
            HttpContext http,
            SystemCatalogService catalog,
            string userName) =>
        {
            var authResult = Authorize(http, catalog, GlobalPermission.ManageUsers);
            if (authResult.Result != null)
                return authResult.Result;

            if (string.IsNullOrWhiteSpace(userName))
                return Results.BadRequest(new { error = "userName is required." });

            catalog.DeleteUser(userName);
            return Results.Ok(new { userName, deleted = true });
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

public sealed class UpsertAdminUserRequest
{
    public string UserName { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string GlobalPermissions { get; set; } = GlobalPermission.None.ToString();
    public Dictionary<string, string> DatabasePermissions { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
