using System.Globalization;
using System.Text.Json.Nodes;

namespace CatDb.AdminUi.Infrastructure;

/// <summary>
/// Converts a raw string typed into a UI form field into the JSON scalar kind CatDb.Server's
/// ScalarConvert.FromJson expects (JSON numbers must stay numbers, booleans must stay booleans —
/// see QueryModels.cs's ScalarConvert.FromJson, which calls JsonElement.GetInt32()/GetBoolean()
/// and throws if the element is a JSON string).
/// </summary>
public static class JsonScalarCoercion
{
    public static JsonNode? ToJsonNode(string jsonSchemaType, string? format, string? rawText)
    {
        if (rawText is null)
            return null;

        return jsonSchemaType.ToLowerInvariant() switch
        {
            "integer" => JsonValue.Create(long.Parse(rawText, CultureInfo.InvariantCulture)),
            "number" => JsonValue.Create(double.Parse(rawText, CultureInfo.InvariantCulture)),
            "boolean" => JsonValue.Create(bool.Parse(rawText)),
            "object" or "array" => JsonNode.Parse(rawText),
            _ => JsonValue.Create(rawText),
        };
    }
}
