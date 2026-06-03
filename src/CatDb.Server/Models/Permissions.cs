// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
