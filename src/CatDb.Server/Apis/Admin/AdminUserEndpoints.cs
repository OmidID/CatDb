// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Server.Auth;
using CatDb.Server.Models;
using CatDb.Server.Services;

namespace CatDb.Server.Apis.Admin;

public static class AdminUserEndpoints
{
    public static IEndpointRouteBuilder MapAdminUserEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/v1/admin/users")
            .RequireAuthorization(PolicyNames.ManageUsers);

        group.MapGet("/", (
            SystemCatalogService catalog,
            int page = 1,
            int pageSize = 20) =>
        {
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
            SystemCatalogService catalog,
            UpsertAdminUserRequest request) =>
        {
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
            SystemCatalogService catalog,
            string userName) =>
        {
            if (string.IsNullOrWhiteSpace(userName))
                return Results.BadRequest(new { error = "userName is required." });

            catalog.DeleteUser(userName);
            return Results.Ok(new { userName, deleted = true });
        });

        return app;
    }
}
