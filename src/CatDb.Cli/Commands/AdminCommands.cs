// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.CommandLine;
using CatDb.Cli.Api;
using CatDb.Cli.Output;

namespace CatDb.Cli.Commands;

// Mirrors AdminDatabaseEndpoints / AdminUserEndpoints on CatDb.Server, one command per route.

public sealed class DbListCommand(IAdminClient admin, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var pageOption = new Option<int>("--page") { DefaultValueFactory = _ => 1 };
        var pageSizeOption = new Option<int>("--page-size") { DefaultValueFactory = _ => 20 };

        var command = new Command("db-list", "List databases.");
        command.Add(pageOption);
        command.Add(pageSizeOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var result = await admin.ListDatabasesAsync(
                parseResult.GetValue(pageOption), parseResult.GetValue(pageSizeOption), ct).ConfigureAwait(false);
            output.WriteRows(result.Items);
        }));

        return command;
    }
}

public sealed class DbCreateCommand(IAdminClient admin, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var nameArg = new Argument<string>("name") { Description = "Database name" };

        var command = new Command("db-create", "Create a database.");
        command.Add(nameArg);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var name = parseResult.GetValue(nameArg)!;
            await admin.CreateDatabaseAsync(name, ct).ConfigureAwait(false);
            output.WriteMessage($"Database '{name}' created.");
        }));

        return command;
    }
}

public sealed class DbDeleteCommand(IAdminClient admin, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var nameArg = new Argument<string>("name") { Description = "Database name" };
        var yesOption = new Option<bool>("--yes", "-y") { Description = "Skip the confirmation prompt" };

        var command = new Command("db-delete", "Delete a database and everything in it.");
        command.Add(nameArg);
        command.Add(yesOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var name = parseResult.GetValue(nameArg)!;
            if (!Confirmation.Confirm($"Delete database '{name}' and all of its tables?", parseResult.GetValue(yesOption)))
            {
                output.WriteMessage("Cancelled.");
                return;
            }

            await admin.DeleteDatabaseAsync(name, ct).ConfigureAwait(false);
            output.WriteMessage($"Database '{name}' deleted.");
        }));

        return command;
    }
}

public sealed class UserListCommand(IAdminClient admin, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var pageOption = new Option<int>("--page") { DefaultValueFactory = _ => 1 };
        var pageSizeOption = new Option<int>("--page-size") { DefaultValueFactory = _ => 20 };

        var command = new Command("user-list", "List users.");
        command.Add(pageOption);
        command.Add(pageSizeOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var result = await admin.ListUsersAsync(
                parseResult.GetValue(pageOption), parseResult.GetValue(pageSizeOption), ct).ConfigureAwait(false);
            output.WriteRows(result.Items);
        }));

        return command;
    }
}

/// <summary>Creates a new user, or overwrites an existing one's password/permissions — the server
/// endpoint (<c>POST /api/v1/admin/users</c>) is an upsert.</summary>
public sealed class UserUpsertCommand(IAdminClient admin, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var userArg = new Argument<string>("username") { Description = "Username to create or update" };
        var passwordOption = new Option<string?>("--password", "-p") { Description = "Password (omit to be prompted)" };
        var globalOption = new Option<string>("--global-permissions", "-g")
        {
            Description = "Comma-separated GlobalPermission flags: None, ListDatabases, ManageDatabases, ManageUsers, Admin.",
            DefaultValueFactory = _ => "None",
        };
        var dbPermOption = new Option<string[]>("--db-permission")
        {
            Description = "Per-database permission, repeatable: --db-permission mydb=Read,Write. " +
                           "Values: None, Read, Write, TableAdmin, HeapAccess, Admin.",
            DefaultValueFactory = _ => [],
            AllowMultipleArgumentsPerToken = true,
        };

        var command = new Command("user-upsert", "Create a user, or update an existing user's password/permissions.");
        command.Add(userArg);
        command.Add(passwordOption);
        command.Add(globalOption);
        command.Add(dbPermOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var userName = parseResult.GetValue(userArg)!;
            var password = parseResult.GetValue(passwordOption)
                ?? Session.MaskedConsole.ReadPassword($"Password for '{userName}': ");

            var databasePermissions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in parseResult.GetValue(dbPermOption) ?? [])
            {
                var parts = entry.Split('=', 2);
                if (parts.Length != 2 || parts[0].Length == 0)
                    throw new InvalidOperationException($"--db-permission must look like 'dbname=Read,Write', got '{entry}'.");
                databasePermissions[parts[0]] = parts[1];
            }

            await admin.UpsertUserAsync(new UpsertUserRequest(
                userName, password, parseResult.GetValue(globalOption)!, databasePermissions), ct).ConfigureAwait(false);
            output.WriteMessage($"User '{userName}' saved.");
        }));

        return command;
    }
}

public sealed class UserDeleteCommand(IAdminClient admin, IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var userArg = new Argument<string>("username") { Description = "Username to delete" };
        var yesOption = new Option<bool>("--yes", "-y") { Description = "Skip the confirmation prompt" };

        var command = new Command("user-delete", "Delete a user.");
        command.Add(userArg);
        command.Add(yesOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var userName = parseResult.GetValue(userArg)!;
            if (!Confirmation.Confirm($"Delete user '{userName}'?", parseResult.GetValue(yesOption)))
            {
                output.WriteMessage("Cancelled.");
                return;
            }

            await admin.DeleteUserAsync(userName, ct).ConfigureAwait(false);
            output.WriteMessage($"User '{userName}' deleted.");
        }));

        return command;
    }
}
