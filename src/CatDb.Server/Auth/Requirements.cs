// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Server.Models;
using Microsoft.AspNetCore.Authorization;

namespace CatDb.Server.Auth;

public sealed class GlobalPermissionRequirement(GlobalPermission permission) : IAuthorizationRequirement
{
    public GlobalPermission Permission { get; } = permission;
}

public sealed class DatabaseReadRequirement : IAuthorizationRequirement;
