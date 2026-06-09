// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Server.Auth;

public static class PolicyNames
{
    public const string ManageUsers = "ManageUsers";
    public const string ManageDatabases = "ManageDatabases";
    public const string ListDatabases = "ListDatabases";
    public const string DatabaseRead = "DatabaseRead";
    public const string DatabaseWrite = "DatabaseWrite";
    public const string DatabaseTableAdmin = "DatabaseTableAdmin";
}
