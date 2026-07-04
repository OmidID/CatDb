// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Text.Json;

namespace CatDb.Cli.Commands;

/// <summary>
/// Parses a <c>--key</c>/<c>--value</c> CLI argument into the <see cref="JsonElement"/> the data API
/// expects. <c>42</c>, <c>true</c> and <c>{"id":1}</c> parse as their JSON scalar/object; anything that
/// isn't valid JSON on its own (e.g. a bare <c>ada</c>) is treated as a JSON string, so users don't have
/// to remember to quote plain string keys/values on the command line.
/// </summary>
public static class JsonArg
{
    public static JsonElement Parse(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            using var doc = JsonDocument.Parse(JsonSerializer.Serialize(raw));
            return doc.RootElement.Clone();
        }
    }

    /// <summary>Loads a JSON Schema document from either an inline JSON literal or, when
    /// <paramref name="raw"/> starts with '@', a file path (e.g. <c>@schema.json</c>).</summary>
    public static JsonElement LoadSchema(string raw)
    {
        var json = raw.StartsWith('@') ? File.ReadAllText(raw[1..]) : raw;
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }
}
