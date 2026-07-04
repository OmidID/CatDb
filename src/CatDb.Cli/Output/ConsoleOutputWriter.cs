// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Reflection;
using System.Text.Json;
using CatDb.Cli.Api;

namespace CatDb.Cli.Output;

public sealed class ConsoleOutputWriter : IOutputWriter
{
    public OutputFormat Format { get; set; } = OutputFormat.Text;

    public void WriteResult<T>(T value)
    {
        if (Format == OutputFormat.Json)
        {
            Console.WriteLine(JsonSerializer.Serialize(value, JsonDefaults.Options));
            return;
        }

        if (value is null)
        {
            Console.WriteLine("(none)");
            return;
        }

        foreach (var prop in Properties<T>())
            Console.WriteLine($"{prop.Name,-20}: {Render(prop.GetValue(value))}");
    }

    public void WriteRows<T>(IReadOnlyCollection<T> rows)
    {
        if (Format == OutputFormat.Json)
        {
            Console.WriteLine(JsonSerializer.Serialize(rows, JsonDefaults.Options));
            return;
        }

        if (rows.Count == 0)
        {
            Console.WriteLine("(no rows)");
            return;
        }

        var props = Properties<T>();
        var cells = rows.Select(row => props.Select(p => Render(p.GetValue(row))).ToArray()).ToList();
        var widths = props.Select((p, i) => Math.Max(p.Name.Length, cells.Count == 0 ? 0 : cells.Max(r => r[i].Length))).ToArray();

        WriteRow(props.Select(p => p.Name).ToArray(), widths);
        Console.WriteLine(string.Join("-+-", widths.Select(w => new string('-', w))));
        foreach (var row in cells)
            WriteRow(row, widths);

        Console.WriteLine($"({rows.Count} row{(rows.Count == 1 ? "" : "s")})");
    }

    public void WriteMessage(string message)
    {
        if (Format == OutputFormat.Json)
            Console.WriteLine(JsonSerializer.Serialize(new { message }, JsonDefaults.Options));
        else
            Console.WriteLine(message);
    }

    public void WriteError(string message)
    {
        if (Format == OutputFormat.Json)
            Console.Error.WriteLine(JsonSerializer.Serialize(new { error = message }, JsonDefaults.Options));
        else
            Console.Error.WriteLine($"Error: {message}");
    }

    private static void WriteRow(IReadOnlyList<string> cells, IReadOnlyList<int> widths)
    {
        Console.WriteLine(string.Join(" | ", cells.Select((c, i) => c.PadRight(widths[i]))));
    }

    private static PropertyInfo[] Properties<T>() =>
        typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

    private static string Render(object? value) => value switch
    {
        null => "",
        JsonElement el => el.GetRawText(),
        string s => s,
        System.Collections.IEnumerable list and not string =>
            JsonSerializer.Serialize(value, JsonDefaults.Options),
        _ => value.ToString() ?? "",
    };
}
