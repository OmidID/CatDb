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
    protected override Task HandleRequirementAsync(
        AuthorizationHandlerContext context,
        DatabaseReadRequirement requirement)
    {
        var httpContext = context.Resource as HttpContext;
        if (httpContext?.Items["AuthenticatedUser"] is not AuthenticatedUser user)
        {
            context.Fail();
            return Task.CompletedTask;
        }

        // Extract database name from route values.
        var databaseName = httpContext.Request.RouteValues["databaseName"]?.ToString();
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            context.Fail();
            return Task.CompletedTask;
        }

        if (user.HasDatabase(databaseName, DatabasePermission.Read))
            context.Succeed(requirement);

        return Task.CompletedTask;
    }
}
