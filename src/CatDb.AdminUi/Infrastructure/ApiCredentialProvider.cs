using System.Net.Http.Headers;
using System.Text;

namespace CatDb.AdminUi.Infrastructure;

/// <summary>
/// Holds the Basic-auth credentials for the current Blazor circuit (scoped lifetime).
/// Populated by <see cref="CatDbAuthenticationStateProvider"/> after a successful sign-in.
/// </summary>
public sealed class ApiCredentialProvider
{
    public string? UserName { get; private set; }

    private string? Password { get; set; }

    public bool IsAuthenticated => UserName is not null;

    public void SetCredentials(string userName, string password)
    {
        UserName = userName;
        Password = password;
    }

    public void Clear()
    {
        UserName = null;
        Password = null;
    }

    /// <summary>Builds the "Authorization: Basic ..." header for the stored credentials, or null if signed out.</summary>
    public AuthenticationHeaderValue? BuildAuthorizationHeader()
    {
        if (UserName is null || Password is null)
            return null;

        return BuildAuthorizationHeader(UserName, Password);
    }

    public static AuthenticationHeaderValue BuildAuthorizationHeader(string userName, string password)
    {
        var raw = Encoding.UTF8.GetBytes($"{userName}:{password}");
        return new AuthenticationHeaderValue("Basic", Convert.ToBase64String(raw));
    }
}
