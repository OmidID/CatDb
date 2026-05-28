using CatDb.Server.Models;
using Microsoft.AspNetCore.Authorization;

namespace CatDb.Server.Auth;

public sealed class GlobalPermissionRequirement(GlobalPermission permission) : IAuthorizationRequirement
{
    public GlobalPermission Permission { get; } = permission;
}

public sealed class DatabaseReadRequirement : IAuthorizationRequirement;
