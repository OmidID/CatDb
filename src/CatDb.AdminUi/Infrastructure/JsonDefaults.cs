using System.Text.Json;

namespace CatDb.AdminUi.Infrastructure;

/// <summary>
/// Shared <see cref="JsonSerializerOptions"/> matching the CatDb.Server API, which serializes
/// responses using ASP.NET Core's default "Web" conventions (camelCase, case-insensitive on read).
/// </summary>
public static class JsonDefaults
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
