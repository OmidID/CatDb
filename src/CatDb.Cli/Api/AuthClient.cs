// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Net;
using CatDb.Cli.Session;

namespace CatDb.Cli.Api;

/// <summary>Validates a candidate username/password pair against the CatDb.Server Basic-auth endpoint.</summary>
public interface IAuthClient
{
    Task<bool> ValidateCredentialsAsync(CliSession session, CancellationToken ct = default);
}

/// <summary>
/// Probes <c>GET /api/v1/admin/databases</c> with the candidate credentials. There is no dedicated
/// login/whoami endpoint on CatDb.Server, so this reuses the cheapest existing route: 401 means the
/// credentials themselves were rejected, while 200/403 both mean the server authenticated the user
/// (403 only means that user lacks the ListDatabases global permission).
/// </summary>
public sealed class AuthClient(HttpClient http) : IAuthClient
{
    public async Task<bool> ValidateCredentialsAsync(CliSession session, CancellationToken ct = default)
    {
        var baseUri = new Uri(session.ServerUrl!.TrimEnd('/') + "/");
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(baseUri, "api/v1/admin/databases/?page=1&pageSize=1"))
        {
            Headers = { Authorization = ApiClientBase.BuildAuthorizationHeader(session.UserName ?? "", session.Password ?? "") }
        };

        using var response = await http.SendAsync(request, ct).ConfigureAwait(false);
        return response.StatusCode != HttpStatusCode.Unauthorized;
    }
}
