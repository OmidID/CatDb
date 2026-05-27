namespace CatDb.Server.Services;

[Flags]
public enum GlobalPermission
{
    None = 0,
    ListDatabases = 1,
    ManageDatabases = 2,
    ManageUsers = 4,
    Admin = 1024,
}

[Flags]
public enum DatabasePermission
{
    None = 0,
    Read = 1,
    Write = 2,
    TableAdmin = 4,
    HeapAccess = 8,
    Admin = 1024,
}

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

public sealed class SystemUserRecord
{
    public string UserName { get; set; } = string.Empty;
    public string PasswordHash { get; set; } = string.Empty;
    public string GlobalPermissions { get; set; } = string.Empty;
    public string DatabasePermissions { get; set; } = string.Empty;
}

public sealed class SystemDatabaseRecord
{
    public string Name { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
}

public sealed class SystemUserView
{
    public string UserName { get; set; } = string.Empty;
    public string GlobalPermissions { get; set; } = string.Empty;
    public string DatabasePermissions { get; set; } = string.Empty;
}
