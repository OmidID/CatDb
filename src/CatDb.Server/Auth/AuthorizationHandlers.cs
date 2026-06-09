// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Server.Models;
using Microsoft.AspNetCore.Authorization;

namespace CatDb.Server.Auth;

public sealed class GlobalPermissionHandler : AuthorizationHandler<GlobalPermissionRequirement>
{
    protected override Task HandleRequirementAsync(
        AuthorizationHandlerContext context,
        GlobalPermissionRequirement requirement)
    {
        var httpContext = context.Resource as HttpContext;
        if (httpContext?.Items["AuthenticatedUser"] is not AuthenticatedUser user)
        {
            context.Fail();
            return Task.CompletedTask;
        }

        if (user.HasGlobal(requirement.Permission))
            context.Succeed(requirement);

        return Task.CompletedTask;
    }
}

public sealed class DatabaseReadHandler : AuthorizationHandler<DatabaseReadRequirement>
{
    protected override Task HandleRequirementAsync(AuthorizationHandlerContext ctx, DatabaseReadRequirement req)
    {
        if (!DbAuth.TryGet(ctx, out var user, out var db)) { ctx.Fail(); return Task.CompletedTask; }
        if (user.HasDatabase(db, DatabasePermission.Read)) ctx.Succeed(req);
        return Task.CompletedTask;
    }
}

public sealed class DatabaseWriteHandler : AuthorizationHandler<DatabaseWriteRequirement>
{
    protected override Task HandleRequirementAsync(AuthorizationHandlerContext ctx, DatabaseWriteRequirement req)
    {
        if (!DbAuth.TryGet(ctx, out var user, out var db)) { ctx.Fail(); return Task.CompletedTask; }
        if (user.HasDatabase(db, DatabasePermission.Write)) ctx.Succeed(req);
        return Task.CompletedTask;
    }
}

public sealed class DatabaseTableAdminHandler : AuthorizationHandler<DatabaseTableAdminRequirement>
{
    protected override Task HandleRequirementAsync(AuthorizationHandlerContext ctx, DatabaseTableAdminRequirement req)
    {
        if (!DbAuth.TryGet(ctx, out var user, out var db)) { ctx.Fail(); return Task.CompletedTask; }
        if (user.HasDatabase(db, DatabasePermission.TableAdmin)) ctx.Succeed(req);
        return Task.CompletedTask;
    }
}

file static class DbAuth
{
    internal static bool TryGet(
        AuthorizationHandlerContext ctx,
        out AuthenticatedUser user,
        out string databaseName)
    {
        user = null!;
        databaseName = string.Empty;
        var http = ctx.Resource as HttpContext;
        if (http?.Items["AuthenticatedUser"] is not AuthenticatedUser u) return false;
        var db = http.Request.RouteValues["databaseName"]?.ToString();
        if (string.IsNullOrWhiteSpace(db)) return false;
        user = u;
        databaseName = db;
        return true;
    }
}
