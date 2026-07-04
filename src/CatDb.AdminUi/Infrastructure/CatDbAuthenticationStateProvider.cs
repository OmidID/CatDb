using System.Security.Claims;
using Microsoft.AspNetCore.Components.Authorization;

namespace CatDb.AdminUi.Infrastructure;

/// <summary>
/// Scoped (per-circuit) authentication state for the Blazor Server app. Wraps the CatDb.Server
/// Basic-auth credentials so the rest of the app can use the standard &lt;AuthorizeView&gt;/
/// [Authorize] pieces instead of ad-hoc "is logged in" checks scattered across pages.
/// </summary>
public sealed class CatDbAuthenticationStateProvider(IAuthClient authClient, ApiCredentialProvider credentials)
    : AuthenticationStateProvider
{
    private static readonly ClaimsPrincipal Anonymous = new(new ClaimsIdentity());

    private ClaimsPrincipal _current = Anonymous;

    public override Task<AuthenticationState> GetAuthenticationStateAsync() =>
        Task.FromResult(new AuthenticationState(_current));

    public async Task<bool> SignInAsync(string userName, string password, CancellationToken ct = default)
    {
        if (!await authClient.ValidateCredentialsAsync(userName, password, ct).ConfigureAwait(false))
            return false;

        credentials.SetCredentials(userName, password);
        _current = new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim(ClaimTypes.Name, userName)], authenticationType: "Basic"));

        NotifyAuthenticationStateChanged(Task.FromResult(new AuthenticationState(_current)));
        return true;
    }

    public void SignOut()
    {
        credentials.Clear();
        _current = Anonymous;
        NotifyAuthenticationStateChanged(Task.FromResult(new AuthenticationState(_current)));
    }
}
