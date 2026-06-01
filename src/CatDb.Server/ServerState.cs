// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Server;

/// <summary>Shared runtime state exposed to the health check and any future endpoints.</summary>
public sealed class ServerState
{
    public bool   IsRunning { get; set; }
    public int    Port      { get; set; }
    public string DatabaseDirectory { get; set; } = string.Empty;
    public string DefaultDatabaseName { get; set; } = string.Empty;
}
