// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Server.Models;

public sealed class UpsertAdminUserRequest
{
    public string UserName { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string GlobalPermissions { get; set; } = GlobalPermission.None.ToString();
    public Dictionary<string, string> DatabasePermissions { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
