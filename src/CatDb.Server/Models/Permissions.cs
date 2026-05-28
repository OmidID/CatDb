namespace CatDb.Server.Models;

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
