namespace CatDb.AdminUi.Options;

/// <summary>Binds the "CatDbApi" configuration section (base URL of the CatDb.Server HTTP API).</summary>
public sealed class CatDbApiOptions
{
    public const string SectionName = "CatDbApi";

    public string BaseUrl { get; set; } = "http://localhost:5100";
}
