// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.CommandLine;
using CatDb.Cli.Api;
using CatDb.Cli.Output;
using CatDb.Cli.Session;

namespace CatDb.Cli.Commands;

// Mirrors DataTableManagementEndpoints on CatDb.Server: table CRUD + index management, one
// command per route. All take an explicit --db, falling back to the session's "current database"
// (see UseCommand) so it doesn't have to be repeated on every call within one REPL session.

/// <summary>Base for every table/index command: resolves --db against the session default and
/// fails clearly instead of calling the API with a null/empty database name.</summary>
public abstract class TableCommandBase(CliSession session)
{
    protected static Option<string?> DbOption() =>
        new("--db", "-d") { Description = "Database name (defaults to the session's current database, see 'use')" };

    protected string ResolveDatabase(string? explicitDb) =>
        explicitDb
        ?? session.CurrentDatabase
        ?? throw new InvalidOperationException("No database specified. Pass --db or run 'use <database>' first.");
}

public sealed class TableListCommand(ITablesClient tables, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var command = new Command("table-list", "List tables in a database.");
        command.Add(dbOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var result = await tables.ListTablesAsync(db, ct).ConfigureAwait(false);
            output.WriteRows(result.Tables);
        }));

        return command;
    }
}

public sealed class TableCreateCommand(ITablesClient tables, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var nameArg = new Argument<string>("name") { Description = "Table name" };
        var keySchemaOption = new Option<string>("--key-schema")
        { Description = "JSON Schema for the key: inline JSON, or @path/to/file.json", Required = true };
        var valueSchemaOption = new Option<string>("--value-schema")
        { Description = "JSON Schema for the value: inline JSON, or @path/to/file.json", Required = true };

        var command = new Command("table-create", "Create a table.");
        command.Add(dbOption);
        command.Add(nameArg);
        command.Add(keySchemaOption);
        command.Add(valueSchemaOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var name = parseResult.GetValue(nameArg)!;
            var request = new CreateTableRequest(
                name,
                JsonArg.LoadSchema(parseResult.GetValue(keySchemaOption)!),
                JsonArg.LoadSchema(parseResult.GetValue(valueSchemaOption)!));

            var info = await tables.CreateTableAsync(db, request, ct).ConfigureAwait(false);
            output.WriteResult(info);
        }));

        return command;
    }
}

public sealed class TableDescribeCommand(ITablesClient tables, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var nameArg = new Argument<string>("name") { Description = "Table name" };

        var command = new Command("table-describe", "Show a table's schema and indexes.");
        command.Add(dbOption);
        command.Add(nameArg);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var info = await tables.GetTableAsync(db, parseResult.GetValue(nameArg)!, ct).ConfigureAwait(false);
            output.WriteResult(info);
        }));

        return command;
    }
}

public sealed class TableDeleteCommand(ITablesClient tables, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var nameArg = new Argument<string>("name") { Description = "Table name" };
        var yesOption = new Option<bool>("--yes", "-y") { Description = "Skip the confirmation prompt" };

        var command = new Command("table-delete", "Delete a table and all its rows/indexes.");
        command.Add(dbOption);
        command.Add(nameArg);
        command.Add(yesOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var name = parseResult.GetValue(nameArg)!;
            if (!Confirmation.Confirm($"Delete table '{name}' from '{db}'?", parseResult.GetValue(yesOption)))
            {
                output.WriteMessage("Cancelled.");
                return;
            }

            await tables.DeleteTableAsync(db, name, ct).ConfigureAwait(false);
            output.WriteMessage($"Table '{name}' deleted from '{db}'.");
        }));

        return command;
    }
}

public sealed class IndexListCommand(ITablesClient tables, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var tableArg = new Argument<string>("table") { Description = "Table name" };

        var command = new Command("index-list", "List indexes on a table.");
        command.Add(dbOption);
        command.Add(tableArg);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var indexes = await tables.ListIndexesAsync(db, parseResult.GetValue(tableArg)!, ct).ConfigureAwait(false);
            output.WriteRows(indexes);
        }));

        return command;
    }
}

public sealed class IndexCreateCommand(ITablesClient tables, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var tableArg = new Argument<string>("table") { Description = "Table name" };
        var nameArg = new Argument<string>("index") { Description = "Index name" };
        var membersOption = new Option<string[]>("--members")
        {
            Description = "Comma or space-separated member field names, in order, e.g. --members City Age",
            AllowMultipleArgumentsPerToken = true,
            Required = true,
        };
        var typeOption = new Option<string>("--type")
        {
            Description = "Unique or NonUnique",
            DefaultValueFactory = _ => IndexTypes.NonUnique,
        };

        var command = new Command("index-create", "Create a secondary index on a table.");
        command.Add(dbOption);
        command.Add(tableArg);
        command.Add(nameArg);
        command.Add(membersOption);
        command.Add(typeOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var members = parseResult.GetValue(membersOption)!
                .SelectMany(m => m.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                .ToList();

            var request = new CreateIndexRequest(parseResult.GetValue(nameArg)!, members, parseResult.GetValue(typeOption)!);
            var info = await tables.CreateIndexAsync(db, parseResult.GetValue(tableArg)!, request, ct).ConfigureAwait(false);
            output.WriteResult(info);
        }));

        return command;
    }
}

public sealed class IndexDropCommand(ITablesClient tables, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var tableArg = new Argument<string>("table") { Description = "Table name" };
        var nameArg = new Argument<string>("index") { Description = "Index name" };
        var yesOption = new Option<bool>("--yes", "-y") { Description = "Skip the confirmation prompt" };

        var command = new Command("index-drop", "Drop a secondary index.");
        command.Add(dbOption);
        command.Add(tableArg);
        command.Add(nameArg);
        command.Add(yesOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var table = parseResult.GetValue(tableArg)!;
            var index = parseResult.GetValue(nameArg)!;
            if (!Confirmation.Confirm($"Drop index '{index}' on '{db}.{table}'?", parseResult.GetValue(yesOption)))
            {
                output.WriteMessage("Cancelled.");
                return;
            }

            await tables.DropIndexAsync(db, table, index, ct).ConfigureAwait(false);
            output.WriteMessage($"Index '{index}' dropped from '{db}.{table}'.");
        }));

        return command;
    }
}

public sealed class IndexRebuildCommand(ITablesClient tables, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var tableArg = new Argument<string>("table") { Description = "Table name" };
        var nameOption = new Option<string?>("--index") { Description = "Index name (omit to rebuild all indexes on the table)" };

        var command = new Command("index-rebuild", "Rebuild one index, or all indexes on a table.");
        command.Add(dbOption);
        command.Add(tableArg);
        command.Add(nameOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var table = parseResult.GetValue(tableArg)!;
            var index = parseResult.GetValue(nameOption);
            await tables.RebuildIndexAsync(db, table, index, ct).ConfigureAwait(false);
            output.WriteMessage(index is null
                ? $"All indexes on '{db}.{table}' rebuilt."
                : $"Index '{index}' on '{db}.{table}' rebuilt.");
        }));

        return command;
    }
}
