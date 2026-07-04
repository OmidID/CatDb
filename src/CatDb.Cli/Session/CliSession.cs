// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Cli.Session;

/// <summary>
/// Mutable, DI-singleton connection state for the running CLI process: server address, current
/// credentials, and the "current database" the REPL's <c>use</c> command points at. Nothing here is
/// ever persisted to disk — every process invocation must supply credentials or be prompted for
/// them (see <see cref="CredentialResolver"/>).
/// </summary>
public sealed class CliSession
{
    public string? ServerUrl { get; set; }
    public string? UserName { get; set; }
    public string? Password { get; set; }
    public string? CurrentDatabase { get; set; }

    public bool IsAuthenticated => !string.IsNullOrWhiteSpace(ServerUrl)
        && !string.IsNullOrWhiteSpace(UserName)
        && Password is not null;

    public void Clear()
    {
        UserName = null;
        Password = null;
        CurrentDatabase = null;
    }
}
