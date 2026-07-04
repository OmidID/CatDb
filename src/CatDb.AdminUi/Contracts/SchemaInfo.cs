using System.Text.Json;

namespace CatDb.AdminUi.Contracts;

/// <summary>One field of a table's key/value JSON Schema (draft-07 subset used by CatDb.Server).</summary>
public sealed record SchemaField(string Name, string JsonType, string? Format);

/// <summary>
/// Client-side reader for the JSON Schema shapes CatDb.Server emits/accepts
/// (see TableManagementService.ParseJsonSchema/ToJsonSchema on the server).
/// </summary>
public static class SchemaInfo
{
    public static string GetType(JsonElement schema) =>
        schema.ValueKind == JsonValueKind.Object && schema.TryGetProperty("type", out var t)
            ? t.GetString() ?? "string"
            : "string";

    public static string? GetFormat(JsonElement schema) =>
        schema.ValueKind == JsonValueKind.Object && schema.TryGetProperty("format", out var f)
            ? f.GetString()
            : null;

    public static bool IsObject(JsonElement schema) => GetType(schema) == "object";

    /// <summary>Top-level fields of an object schema, in declared order. Empty for primitive/array schemas.</summary>
    public static List<SchemaField> GetFields(JsonElement schema)
    {
        var fields = new List<SchemaField>();
        if (!IsObject(schema) || !schema.TryGetProperty("properties", out var props) ||
            props.ValueKind != JsonValueKind.Object)
            return fields;

        var order = new List<string>();
        if (schema.TryGetProperty("required", out var req) && req.ValueKind == JsonValueKind.Array)
            foreach (var r in req.EnumerateArray())
                if (r.GetString() is { } n) order.Add(n);

        foreach (var p in props.EnumerateObject())
            if (!order.Contains(p.Name, StringComparer.Ordinal))
                order.Add(p.Name);

        foreach (var name in order)
        {
            if (!props.TryGetProperty(name, out var fieldSchema))
                continue;
            fields.Add(new SchemaField(name, GetType(fieldSchema), GetFormat(fieldSchema)));
        }

        return fields;
    }

    /// <summary>Renders a schema back to a single-line human summary, e.g. "object { Name: string, Age: integer }".</summary>
    public static string Describe(JsonElement schema)
    {
        var type = GetType(schema);
        if (type != "object")
        {
            var format = GetFormat(schema);
            return format is null ? type : $"{type} ({format})";
        }

        var fields = GetFields(schema).Select(f => $"{f.Name}: {f.JsonType}");
        return $"object {{ {string.Join(", ", fields)} }}";
    }
}
