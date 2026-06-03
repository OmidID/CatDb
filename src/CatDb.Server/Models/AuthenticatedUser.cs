// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Server.Models;

public sealed class AuthenticatedUser
{
    public required string UserName { get; init; }
    public required GlobalPermission GlobalPermissions { get; init; }
    public required Dictionary<string, DatabasePermission> DatabasePermissions { get; init; }

    public bool HasGlobal(GlobalPermission permission)
    {
        return (GlobalPermissions & GlobalPermission.Admin) == GlobalPermission.Admin
            || (GlobalPermissions & permission) == permission;
    }

    public bool HasDatabase(string databaseName, DatabasePermission permission)
    {
        if (DatabasePermissions.TryGetValue("*", out var allDbPerms) && HasDbPermission(allDbPerms, permission))
            return true;

        if (!DatabasePermissions.TryGetValue(databaseName, out var dbPerms))
            return false;

        return HasDbPermission(dbPerms, permission);
    }

    private static bool HasDbPermission(DatabasePermission current, DatabasePermission required)
    {
        return (current & DatabasePermission.Admin) == DatabasePermission.Admin
            || (current & required) == required;
    }
}
