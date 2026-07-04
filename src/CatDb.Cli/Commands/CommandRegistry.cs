// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.CommandLine;

namespace CatDb.Cli.Commands;

/// <summary>
/// Builds the single <see cref="RootCommand"/> tree used for every invocation of the process: the
/// initial argv (which may carry the global connection options below plus, in one-shot mode, a verb)
/// and every REPL line (verb only — see <see cref="Repl.ReplEngine"/>). Global options are declared
/// once here and read directly off the top-level <see cref="ParseResult"/> in Program.cs before any
/// subcommand runs, so they never need to be repeated per line in the REPL.
/// </summary>
public sealed class CommandRegistry(IEnumerable<ICliCommand> commands)
{
    // Recursive so these are recognized regardless of where they're given relative to the verb
    // (System.CommandLine only lets a subcommand see a root option that isn't marked Recursive when
    // it appears BEFORE the verb token; without this, "-o db-list --format json" would fail to parse).
    public static readonly Option<string?> ServerOption =
        new("--server", "-s") { Description = "CatDb.Server base URL, e.g. http://localhost:5000", Recursive = true };

    public static readonly Option<string?> UserOption =
        new("--user", "-u") { Description = "Username", Recursive = true };

    public static readonly Option<string?> PasswordOption = new("--password", "-p")
    {
        Description = "Password. Omit to be prompted (recommended) or set CATDB_PASSWORD instead of " +
                       "passing it here, since CLI arguments can end up in shell history and process listings.",
        Recursive = true,
    };

    public static readonly Option<bool> OperationOption = new("--operation", "-o")
    {
        Description = "Run exactly one command (given after this flag, same syntax as the interactive " +
                       "prompt) and exit, instead of entering interactive mode.",
        Recursive = true,
    };

    public static readonly Option<string> FormatOption = new("--format")
    {
        Description = "Output format for command results: text (default) or json.",
        DefaultValueFactory = _ => "text",
        Recursive = true,
    };

    public RootCommand Build()
    {
        var root = new RootCommand(
            "CatDb command-line client. Run with no arguments for an interactive prompt, or with " +
            "-o <command> ... to run one command and exit.");

        root.Add(ServerOption);
        root.Add(UserOption);
        root.Add(PasswordOption);
        root.Add(OperationOption);
        root.Add(FormatOption);

        foreach (var command in commands)
            root.Add(command.BuildCommand());

        // Without an action of its own, RootCommand treats "no subcommand given" as a parse error
        // ("Required command was not provided") — which would also reject the perfectly valid
        // "-u admin -p admin -s http://host" (connect, then enter interactive mode) invocation.
        // This no-op action makes that a normal, error-free parse; Program.cs tells it apart from a
        // genuine --help/--version action by reference-comparing ParseResult.Action against
        // root.Action (both cases parse with zero errors and CommandResult.Command == root, so the
        // action reference is the only distinguishing signal between them).
        root.SetAction((_, _) => Task.FromResult(0));

        return root;
    }
}
