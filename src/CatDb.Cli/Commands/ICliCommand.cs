// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.CommandLine;

namespace CatDb.Cli.Commands;

/// <summary>
/// Command Pattern contract: one implementation per CLI verb (<c>db-create</c>, <c>row-insert</c>, …).
/// Each command builds its own self-describing <see cref="System.CommandLine.Command"/> (options,
/// arguments, help text) and wires its action to call the relevant typed API client. The same
/// <see cref="Command"/> tree is used for every invocation style: the initial process argv (one-shot
/// <c>-o</c> mode) and every line typed at the REPL prompt (<see cref="Repl.ReplEngine"/>) — see
/// <see cref="CommandRegistry"/>.
/// </summary>
public interface ICliCommand
{
    Command BuildCommand();
}
