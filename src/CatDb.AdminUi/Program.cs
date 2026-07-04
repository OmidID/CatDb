using System.Diagnostics;
using System.Globalization;
using CatDb.AdminUi.Clients;
using CatDb.AdminUi.Components;
using CatDb.AdminUi.Infrastructure;
using CatDb.AdminUi.Options;
using CatDb.AdminUi.Resources;
using Microsoft.AspNetCore.Components.Authorization;
using Microsoft.AspNetCore.Localization;

// A single-file self-contained publish extracts native libs/content to a per-run temp cache dir
// (AppContext.BaseDirectory) that is NOT where wwwroot/the static-assets manifest actually live —
// those sit next to the real executable. Environment.ProcessPath still points at the real exe even
// under single-file, so pin ContentRootPath (and therefore the default WebRootPath) to its directory;
// otherwise MapStaticAssets/wwwroot lookups fail with "static resources manifest file ... was not found".
var appDirectory = Path.GetDirectoryName(Environment.ProcessPath) is { Length: > 0 } dir
    ? dir
    : AppContext.BaseDirectory;
var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    ContentRootPath = appDirectory,
});

// Published single-file/self-contained runs (double-click, no `dotnet run`, no launchSettings) need a
// fixed default address to bind AND to open a browser against. Only applied when nothing else already
// configured a URL (env var, --urls, or appsettings "Kestrel" section) so `dotnet run`/hosting
// overrides still win.
const string DefaultUrl = "http://127.0.0.1:5390";
var hasExplicitUrl = !string.IsNullOrEmpty(builder.Configuration["urls"])
    || !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("ASPNETCORE_URLS"))
    || builder.Configuration.GetSection("Kestrel:Endpoints").Exists();
if (!hasExplicitUrl)
    builder.WebHost.UseUrls(DefaultUrl);

// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services
    .AddOptions<CatDbApiOptions>()
    .Bind(builder.Configuration.GetSection(CatDbApiOptions.SectionName))
    .ValidateDataAnnotations();

// No ResourcesPath here even though the .resx files live in Resources/: the SDK's embedded-resource
// naming for .resx drops the folder segment (verified via reflection: the compiled manifest resource
// name is "CatDb.AdminUi.SharedResources.resources", not "...Resources.SharedResources..."). Setting
// ResourcesPath="Resources" would make IStringLocalizer<T> look for a name that doesn't exist and
// every lookup silently falls back to printing the raw key.
builder.Services.AddLocalization();

// Per-circuit auth state: holds the signed-in user's Basic-auth credentials and exposes them
// through the standard AuthenticationStateProvider so components can use <AuthorizeView> for
// display purposes (e.g. showing the username in NavMenu). Actual page-access gating is done by
// MainLayout directly against ApiCredentialProvider — see the note below.
builder.Services.AddScoped<ApiCredentialProvider>();
builder.Services.AddScoped<ApiConnectionState>();
builder.Services.AddScoped<CatDbAuthenticationStateProvider>();
builder.Services.AddScoped<AuthenticationStateProvider>(sp => sp.GetRequiredService<CatDbAuthenticationStateProvider>());
// Deliberately no FallbackPolicy / [Authorize] endpoint metadata: this app has no ASP.NET Core
// authentication scheme (no Identity, no cookies) — auth is purely a scoped, per-circuit credential
// store forwarded as a Basic-auth header to CatDb.Server. Endpoint-level enforcement would push
// authorization into the HTTP routing pipeline, which calls ChallengeAsync() and needs a real
// IAuthenticationService. Instead, MainLayout gates access directly against ApiCredentialProvider.
builder.Services.AddAuthorizationCore();
builder.Services.AddCascadingAuthenticationState();

// Typed HttpClient per CatDb.Server API area (Microsoft-recommended pattern:
// https://learn.microsoft.com/aspnet/core/fundamentals/http-requests#typed-clients). No fixed
// BaseAddress here — ApiClientBase/AuthClient resolve each request against the current, user-
// configurable ApiConnectionState.BaseUrl (see the Login page's "Server address" field).
builder.Services.AddHttpClient<IAuthClient, AuthClient>();
builder.Services.AddHttpClient<IAdminDatabasesClient, AdminDatabasesClient>();
builder.Services.AddHttpClient<IAdminUsersClient, AdminUsersClient>();
builder.Services.AddHttpClient<ITablesClient, TablesClient>();
builder.Services.AddHttpClient<IDataClient, DataClient>();

var app = builder.Build();

// Double-click/standalone-exe UX: open the default browser once Kestrel is actually listening.
// Skipped in Development so `dotnet run` (launchSettings already has launchBrowser: true) doesn't
// pop two tabs.
if (!app.Environment.IsDevelopment())
{
    app.Lifetime.ApplicationStarted.Register(() =>
    {
        var url = app.Urls.FirstOrDefault() ?? DefaultUrl;
        try
        {
            Process.Start(new ProcessStartInfo(url) { UseShellExecute = true });
        }
        catch
        {
            // Best-effort only — headless/container hosts have no browser to open.
        }
    });
}

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}
app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
app.UseHttpsRedirection();

app.UseRequestLocalization(SupportedCultures.BuildRequestLocalizationOptions());

// Blazor Server can't change the current circuit's thread culture mid-circuit, so switching
// language is a real navigation: LanguageSwitcher posts here, we stamp the culture cookie
// (read by UseRequestLocalization above on the NEXT request) and redirect back.
app.MapGet("/culture/set", (string culture, string redirectUri, HttpContext http) =>
{
    http.Response.Cookies.Append(
        CookieRequestCultureProvider.DefaultCookieName,
        CookieRequestCultureProvider.MakeCookieValue(new RequestCulture(culture)),
        new CookieOptions { Expires = DateTimeOffset.UtcNow.AddYears(1), IsEssential = true });

    return Results.LocalRedirect(redirectUri);
});

app.UseAntiforgery();

// Same single-file caveat as ContentRootPath above: the default overload resolves the manifest via
// the entry assembly location, which is empty/wrong under a single-file publish. Point it at the
// manifest that actually sits next to the real executable.
app.MapStaticAssets(Path.Combine(appDirectory, $"{typeof(Program).Assembly.GetName().Name}.staticwebassets.endpoints.json"));
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
