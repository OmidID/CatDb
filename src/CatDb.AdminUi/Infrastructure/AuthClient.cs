using System.Net;

namespace CatDb.AdminUi.Infrastructure;

/// <summary>Validates a candidate username/password pair against the CatDb.Server Basic-auth endpoint.</summary>
public interface IAuthClient
{
    Task<bool> ValidateCredentialsAsync(string userName, string password, CancellationToken ct = default);
}

/// <summary>
/// Probes <c>GET /api/v1/admin/databases</c> with the candidate credentials, against whatever
/// server address is currently in <see cref="ApiConnectionState"/> (the Login page sets it from its
/// "Server address" field before calling this). There is no dedicated "whoami"/login endpoint, so we
/// reuse the cheapest existing route: 401 means the credentials themselves were rejected, while
/// 200/403 both mean CatDb.Server authenticated the user (403 only means that particular user lacks
/// the ListDatabases global permission).
/// </summary>
public sealed class AuthClient(HttpClient http, ApiConnectionState connection) : IAuthClient
{
    public async Task<bool> ValidateCredentialsAsync(string userName, string password, CancellationToken ct = default)
    {
        var baseUri = new Uri(connection.BaseUrl.TrimEnd('/') + "/");
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(baseUri, "api/v1/admin/databases/?page=1&pageSize=1"))
        {
            Headers = { Authorization = ApiCredentialProvider.BuildAuthorizationHeader(userName, password) }
        };

        using var response = await http.SendAsync(request, ct).ConfigureAwait(false);
        return response.StatusCode != HttpStatusCode.Unauthorized;
    }
}
