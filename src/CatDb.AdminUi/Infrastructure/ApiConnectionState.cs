using CatDb.AdminUi.Options;
using Microsoft.Extensions.Options;

namespace CatDb.AdminUi.Infrastructure;

/// <summary>
/// Scoped (per-circuit) CatDb.Server address. Seeded from <see cref="CatDbApiOptions.BaseUrl"/>
/// (the "CatDbApi:BaseUrl" appsettings default) but overridable per-login from the Login page, so
/// one CatDb.AdminUi instance can point at different servers without a restart.
/// </summary>
public sealed class ApiConnectionState(IOptions<CatDbApiOptions> options)
{
    public string BaseUrl { get; set; } = options.Value.BaseUrl;
}
