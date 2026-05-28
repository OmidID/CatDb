using System.Security.Claims;
using System.Text.Encodings.Web;
using CatDb.Server.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace CatDb.Server.Auth;

public sealed class BasicAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    SystemCatalogService catalog) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public const string SchemeName = "Basic";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!BasicAuthHelpers.TryReadCredentials(Context, out var userName, out var password))
            return Task.FromResult(AuthenticateResult.NoResult());

        var user = catalog.Authenticate(userName, password);
        if (user == null)
            return Task.FromResult(AuthenticateResult.Fail("Invalid credentials."));

        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, user.UserName),
            new("GlobalPermissions", ((int)user.GlobalPermissions).ToString()),
        };

        foreach (var kv in user.DatabasePermissions)
            claims.Add(new Claim("DatabasePermission", $"{kv.Key}={(int)kv.Value}"));

        var identity = new ClaimsIdentity(claims, SchemeName);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, SchemeName);

        // Store the full AuthenticatedUser for downstream use.
        Context.Items["AuthenticatedUser"] = user;

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }

    protected override Task HandleChallengeAsync(AuthenticationProperties properties)
    {
        Response.Headers.WWWAuthenticate = "Basic realm=\"CatDb\"";
        return base.HandleChallengeAsync(properties);
    }
}
