// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.CommandLine;
using System.Text;
using CatDb.Cli.Api;
using CatDb.Cli.Output;
using CatDb.Cli.Session;

namespace CatDb.Cli.Commands;

// Mirrors DataTableEndpoints on CatDb.Server: row browse/query/insert/replace/delete.

public sealed class RowBrowseCommand(IDataClient data, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var tableArg = new Argument<string>("table") { Description = "Table name" };
        var takeOption = new Option<int>("--take") { DefaultValueFactory = _ => 50 };
        var fromOption = new Option<string?>("--from-key") { Description = "Primary-key range lower bound" };
        var toOption = new Option<string?>("--to-key") { Description = "Primary-key range upper bound" };
        var backwardOption = new Option<bool>("--backward") { Description = "Scan in descending key order" };

        var command = new Command("row-browse", "Browse rows by primary-key range (cheap key scan, no filtering).");
        command.Add(dbOption);
        command.Add(tableArg);
        command.Add(takeOption);
        command.Add(fromOption);
        command.Add(toOption);
        command.Add(backwardOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var result = await data.BrowseAsync(
                db, parseResult.GetValue(tableArg)!, parseResult.GetValue(takeOption),
                parseResult.GetValue(fromOption), parseResult.GetValue(toOption),
                parseResult.GetValue(backwardOption) ? "backward" : "forward", ct).ConfigureAwait(false);
            output.WriteRows(result.Rows);
        }));

        return command;
    }
}

/// <summary>Full field-predicate/order/paging query, built into the same query-string grammar
/// CatDb.Server's <c>QueryStringParser</c> parses (see QueryModels.cs on the server for the full
/// grammar this mirrors).</summary>
public sealed class RowQueryCommand(IDataClient data, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var tableArg = new Argument<string>("table") { Description = "Table name" };
        var filterOption = new Option<string[]>("--filter")
        {
            Description = "Repeatable AND predicate 'field=value' or 'field=op:value', " +
                           "e.g. --filter City=nyc --filter Age=gte:30. Ops: eq,gt,gte,lt,lte,between,prefix.",
            DefaultValueFactory = _ => [],
        };
        var orOption = new Option<string[]>("--or")
        {
            Description = "Repeatable OR term 'field:op:value' (single OR-group, ANDed with --filter), " +
                           "e.g. --or City:eq:nyc --or City:eq:la.",
            DefaultValueFactory = _ => [],
        };
        var orderOption = new Option<string[]>("--order")
        {
            Description = "Repeatable sort key: a field name, optionally ':desc' (or '$key' for the primary key).",
            DefaultValueFactory = _ => [],
        };
        var takeOption = new Option<int?>("--take") { Description = "Max rows to return" };
        var skipOption = new Option<int?>("--skip") { Description = "Rows to skip before taking" };
        var countOption = new Option<bool>("--count") { Description = "Include the total match count" };
        var fromKeyOption = new Option<string?>("--from-key") { Description = "Primary-key range lower bound" };
        var toKeyOption = new Option<string?>("--to-key") { Description = "Primary-key range upper bound" };

        var command = new Command("row-query", "Filter/sort/page rows via the query engine.");
        command.Add(dbOption);
        command.Add(tableArg);
        command.Add(filterOption);
        command.Add(orOption);
        command.Add(orderOption);
        command.Add(takeOption);
        command.Add(skipOption);
        command.Add(countOption);
        command.Add(fromKeyOption);
        command.Add(toKeyOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var qs = BuildQueryString(parseResult, filterOption, orOption, orderOption, takeOption, skipOption,
                countOption, fromKeyOption, toKeyOption);

            var result = await data.QueryAsync(db, parseResult.GetValue(tableArg)!, qs, ct).ConfigureAwait(false);
            output.WriteRows(result.Rows);
            if (result.Total is { } total)
                output.WriteMessage($"Total matches: {total}");
        }));

        return command;
    }

    private static string BuildQueryString(
        ParseResult parseResult,
        Option<string[]> filterOption, Option<string[]> orOption, Option<string[]> orderOption,
        Option<int?> takeOption, Option<int?> skipOption, Option<bool> countOption,
        Option<string?> fromKeyOption, Option<string?> toKeyOption)
    {
        var qs = new StringBuilder();

        foreach (var filter in parseResult.GetValue(filterOption) ?? [])
        {
            var parts = filter.Split('=', 2);
            if (parts.Length != 2)
                throw new InvalidOperationException($"--filter must look like 'field=value' or 'field=op:value', got '{filter}'.");
            Append(qs, parts[0], parts[1]);
        }

        var orTerms = parseResult.GetValue(orOption) ?? [];
        if (orTerms.Length > 0)
            Append(qs, "or", $"({string.Join(',', orTerms)})");

        var order = parseResult.GetValue(orderOption) ?? [];
        if (order.Length > 0)
            Append(qs, "order", string.Join(',', order));

        if (parseResult.GetValue(takeOption) is { } take) Append(qs, "take", take.ToString());
        if (parseResult.GetValue(skipOption) is { } skip) Append(qs, "skip", skip.ToString());
        if (parseResult.GetValue(countOption)) Append(qs, "count", "true");
        if (parseResult.GetValue(fromKeyOption) is { } fromKey) Append(qs, "fromKey", fromKey);
        if (parseResult.GetValue(toKeyOption) is { } toKey) Append(qs, "toKey", toKey);

        return qs.ToString();
    }

    private static void Append(StringBuilder qs, string key, string value)
    {
        if (qs.Length > 0) qs.Append('&');
        qs.Append(Uri.EscapeDataString(key)).Append('=').Append(Uri.EscapeDataString(value));
    }
}

public sealed class RowInsertCommand(IDataClient data, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var tableArg = new Argument<string>("table") { Description = "Table name" };
        var keyOption = new Option<string>("--key") { Description = "Row key as JSON (or a bare scalar)", Required = true };
        var valueOption = new Option<string>("--value") { Description = "Row value as JSON (or a bare scalar)", Required = true };

        var command = new Command("row-insert", "Insert a row (no-op if the key already exists).");
        command.Add(dbOption);
        command.Add(tableArg);
        command.Add(keyOption);
        command.Add(valueOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var result = await data.InsertAsync(
                db, parseResult.GetValue(tableArg)!,
                JsonArg.Parse(parseResult.GetValue(keyOption)!),
                JsonArg.Parse(parseResult.GetValue(valueOption)!), ct).ConfigureAwait(false);
            output.WriteResult(result);
        }));

        return command;
    }
}

public sealed class RowPutCommand(IDataClient data, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var tableArg = new Argument<string>("table") { Description = "Table name" };
        var keyOption = new Option<string>("--key") { Description = "Row key as JSON (or a bare scalar)", Required = true };
        var valueOption = new Option<string>("--value") { Description = "Row value as JSON (or a bare scalar)", Required = true };

        var command = new Command("row-put", "Insert or replace a row (upsert).");
        command.Add(dbOption);
        command.Add(tableArg);
        command.Add(keyOption);
        command.Add(valueOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var result = await data.ReplaceAsync(
                db, parseResult.GetValue(tableArg)!,
                JsonArg.Parse(parseResult.GetValue(keyOption)!),
                JsonArg.Parse(parseResult.GetValue(valueOption)!), ct).ConfigureAwait(false);
            output.WriteResult(result);
        }));

        return command;
    }
}

public sealed class RowDeleteCommand(IDataClient data, CliSession session, IOutputWriter output)
    : TableCommandBase(session), ICliCommand
{
    public Command BuildCommand()
    {
        var dbOption = DbOption();
        var tableArg = new Argument<string>("table") { Description = "Table name" };
        var keyOption = new Option<string>("--key") { Description = "Row key as JSON (or a bare scalar)", Required = true };
        var yesOption = new Option<bool>("--yes", "-y") { Description = "Skip the confirmation prompt" };

        var command = new Command("row-delete", "Delete a row by key.");
        command.Add(dbOption);
        command.Add(tableArg);
        command.Add(keyOption);
        command.Add(yesOption);

        command.SetAction(CommandExecution.Wrap(output, async (parseResult, ct) =>
        {
            var db = ResolveDatabase(parseResult.GetValue(dbOption));
            var table = parseResult.GetValue(tableArg)!;
            var keyRaw = parseResult.GetValue(keyOption)!;
            if (!Confirmation.Confirm($"Delete row with key {keyRaw} from '{db}.{table}'?", parseResult.GetValue(yesOption)))
            {
                output.WriteMessage("Cancelled.");
                return;
            }

            await data.DeleteAsync(db, table, JsonArg.Parse(keyRaw), ct).ConfigureAwait(false);
            output.WriteMessage("Row deleted.");
        }));

        return command;
    }
}
