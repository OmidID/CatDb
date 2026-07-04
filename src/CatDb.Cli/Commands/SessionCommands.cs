// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.CommandLine;
using CatDb.Cli.Api;
using CatDb.Cli.Output;
using CatDb.Cli.Session;

namespace CatDb.Cli.Commands;

/// <summary>Connect/reconnect, disconnect, show current identity, and switch the "current database"
/// the <c>row-*</c>/<c>table-*</c>/<c>index-*</c> commands default to. Mirrors psql's <c>\c</c>.</summary>
public sealed class LoginCommand(CliSession session, CredentialResolver credentials, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var serverArg = new Option<string?>("--server", "-s") { Description = "CatDb.Server base URL" };
        var userArg = new Option<string?>("--user", "-u") { Description = "Username" };
        var passwordArg = new Option<string?>("--password", "-p") { Description = "Password (omit to be prompted)" };

        var command = new Command("login", "Connect (or reconnect) to a CatDb.Server as a given user.");
        command.Add(serverArg);
        command.Add(userArg);
        command.Add(passwordArg);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            session.ServerUrl = parseResult.GetValue(serverArg) ?? session.ServerUrl;
            session.UserName = parseResult.GetValue(userArg);
            session.Password = parseResult.GetValue(passwordArg);
            session.CurrentDatabase = null;

            if (!await credentials.ResolveAndValidateAsync(session, ct).ConfigureAwait(false))
            {
                session.Clear();
                throw new InvalidOperationException("Login failed.");
            }

            output.WriteMessage($"Connected to {session.ServerUrl} as '{session.UserName}'.");
        }));

        return command;
    }
}

public sealed class LogoutCommand(CliSession session, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var command = new Command("logout", "Forget the current credentials.");
        command.SetAction(CommandExecution.Wrap(output, (_, _) =>
        {
            session.Clear();
            output.WriteMessage("Logged out.");
            return Task.CompletedTask;
        }));
        return command;
    }
}

public sealed class WhoAmICommand(CliSession session, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var command = new Command("whoami", "Show the server, user and current database for this session.");
        command.SetAction(CommandExecution.Wrap(output, (_, _) =>
        {
            output.WriteResult(new
            {
                Server = session.ServerUrl ?? "(not connected)",
                User = session.UserName ?? "(none)",
                Database = session.CurrentDatabase ?? "(none)",
            });
            return Task.CompletedTask;
        }));
        return command;
    }
}

public sealed class UseCommand(CliSession session, IAdminClient admin, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var nameArg = new Argument<string>("database") { Description = "Database name to make current" };

        var command = new Command("use", "Set the current database for subsequent table/index/row commands.");
        command.Add(nameArg);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var name = parseResult.GetValue(nameArg)!;

            // Best-effort existence check (no single-database lookup endpoint exists); a catalog
            // with more than one page of databases just skips straight to setting it.
            var page = await admin.ListDatabasesAsync(1, 200, ct).ConfigureAwait(false);
            if (page.Total <= page.Items.Count &&
                !page.Items.Any(d => string.Equals(d.Name, name, StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException($"Database '{name}' was not found.");
            }

            session.CurrentDatabase = name;
            output.WriteMessage($"Current database is now '{name}'.");
        }));

        return command;
    }
}
