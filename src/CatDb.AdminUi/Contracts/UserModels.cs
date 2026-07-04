namespace CatDb.AdminUi.Contracts;

/// <summary>
/// <paramref name="DatabasePermissions"/> is a flattened "db=perm;db=perm" string on read
/// (SystemCatalogService.SerializeDatabasePermissions on the server) — asymmetric with the write
/// side, which takes a real dictionary (see <see cref="UpsertUserRequest"/>). Use
/// <see cref="DatabasePermissionsFormat.Parse"/> to turn it into a dictionary for display/editing.
/// </summary>
public sealed record UserView(string UserName, string GlobalPermissions, string DatabasePermissions);

public sealed record UpsertUserRequest(
    string UserName,
    string Password,
    string GlobalPermissions,
    Dictionary<string, string> DatabasePermissions);

/// <summary>Parses the server's flattened "db=perm;db=perm" DatabasePermissions read format.</summary>
public static class DatabasePermissionsFormat
{
    public static Dictionary<string, string> Parse(string value)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var segment in value.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var idx = segment.IndexOf('=');
            if (idx <= 0 || idx >= segment.Length - 1)
                continue;

            result[segment[..idx]] = segment[(idx + 1)..];
        }

        return result;
    }
}

/// <summary>Flag names accepted by the server's <c>GlobalPermission</c> enum (Requests.cs / Permissions.cs).</summary>
public static class GlobalPermissionFlags
{
    public const string ListDatabases = "ListDatabases";
    public const string ManageDatabases = "ManageDatabases";
    public const string ManageUsers = "ManageUsers";
    public const string Admin = "Admin";

    public static readonly IReadOnlyList<string> All = [ListDatabases, ManageDatabases, ManageUsers, Admin];
}

/// <summary>Flag names accepted by the server's per-database <c>DatabasePermission</c> enum.</summary>
public static class DatabasePermissionFlags
{
    public const string Read = "Read";
    public const string Write = "Write";
    public const string TableAdmin = "TableAdmin";
    public const string HeapAccess = "HeapAccess";
    public const string Admin = "Admin";

    public static readonly IReadOnlyList<string> All = [Read, Write, TableAdmin, HeapAccess, Admin];
}
