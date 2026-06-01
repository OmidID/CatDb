// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Server.Models;

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
