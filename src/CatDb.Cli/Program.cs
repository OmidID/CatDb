// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Cli;
using CatDb.Cli.Commands;
using CatDb.Cli.Output;
using CatDb.Cli.Repl;
using CatDb.Cli.Session;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection().AddCatDbCli();
await using var provider = services.BuildServiceProvider();

var root = provider.GetRequiredService<RootCommand>();
var session = provider.GetRequiredService<CliSession>();
var output = provider.GetRequiredService<IOutputWriter>();
var credentials = provider.GetRequiredService<CredentialResolver>();

// CommandRegistry.Build() gave root its own no-op action so "no subcommand" parses cleanly instead
// of erroring (see CommandRegistry.cs) — capture that action's identity now, before Parse, so it can
// be told apart below from a genuine --help/--version action.
var rootOwnAction = root.Action;

var parseResult = root.Parse(args);

output.Format = string.Equals(parseResult.GetValue(CommandRegistry.FormatOption), "json", StringComparison.OrdinalIgnoreCase)
    ? OutputFormat.Json
    : OutputFormat.Text;

session.ServerUrl = parseResult.GetValue(CommandRegistry.ServerOption);
session.UserName = parseResult.GetValue(CommandRegistry.UserOption);
session.Password = parseResult.GetValue(CommandRegistry.PasswordOption);

var noSubcommandGiven = ReferenceEquals(parseResult.CommandResult.Command, root);

// ── A genuine parse error, or --help/--version on the top-level invocation: both are handled
// entirely by System.CommandLine's own built-in actions and need no server connection, so answer
// them before ever prompting for credentials (e.g. bare `catdb --help` must not block on a login
// prompt). Distinguished from "no subcommand, connect and go interactive" by action identity —
// both cases have zero errors and CommandResult.Command == root, but only a real --help/--version
// replaces root's own action.
if (parseResult.Errors.Count > 0 || (noSubcommandGiven && !ReferenceEquals(parseResult.Action, rootOwnAction)))
    return await parseResult.InvokeAsync(new InvocationConfiguration(), CancellationToken.None).ConfigureAwait(false);

// ── -o/--operation: run exactly one command, print its output, exit. ──────────────────────────
if (parseResult.GetValue(CommandRegistry.OperationOption))
{
    if (noSubcommandGiven)
    {
        output.WriteError("-o/--operation needs a command, e.g. -o db-list.");
        return 1;
    }

    using var cts = new CancellationTokenSource();
    return await WithCancelKeyAsync(cts, async ct =>
    {
        if (!await credentials.ResolveAndValidateAsync(session, ct).ConfigureAwait(false))
            return 1;

        return await parseResult.InvokeAsync(new InvocationConfiguration(), ct).ConfigureAwait(false);
    }).ConfigureAwait(false);
}

// ── No -o: interactive mode. Any operation-shaped tokens without -o are a usage error — the
// mode is picked by -o alone, not by "did the user also type a command". ──────────────────────
if (!noSubcommandGiven)
{
    output.WriteError("Arguments were given without -o/--operation. " +
                       "Use -o <command> ... to run one command and exit, or nothing to start the interactive prompt.");
    return 1;
}

// Covers both "catdb" (nothing given, prompt for everything) and "catdb -u admin -p admin -s
// http://host" (creds given, skip straight to the prompt) — CredentialResolver fills in only
// whatever CliSession is still missing.
using var loginCts = new CancellationTokenSource();
var loggedIn = await WithCancelKeyAsync(loginCts, ct => credentials.ResolveAndValidateAsync(session, ct)).ConfigureAwait(false);
if (!loggedIn)
    return 1;

await provider.GetRequiredService<ReplEngine>().RunAsync(CancellationToken.None).ConfigureAwait(false);
return 0;

// Scopes a Ctrl+C handler to exactly one operation, instead of a process-wide handler that would
// also have to be torn down before the REPL installs its own per-line handler (ReplEngine.cs).
static async Task<T> WithCancelKeyAsync<T>(CancellationTokenSource cts, Func<CancellationToken, Task<T>> body)
{
    ConsoleCancelEventHandler onCancel = (_, e) => { e.Cancel = true; cts.Cancel(); };
    Console.CancelKeyPress += onCancel;
    try
    {
        return await body(cts.Token).ConfigureAwait(false);
    }
    finally
    {
        Console.CancelKeyPress -= onCancel;
    }
}
