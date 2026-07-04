// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using CatDb.Cli.Session;

namespace CatDb.Cli.Api;

/// <summary>
/// Common request/response plumbing shared by every typed CatDb.Server API client: resolves the
/// request against the current <see cref="CliSession.ServerUrl"/> (settable at any time via the
/// <c>login</c>/<c>use</c> commands, not fixed at DI-registration time), attaches the Basic-auth
/// header for the signed-in user, and turns non-success responses into one <see cref="ApiException"/>
/// type every command can catch and print consistently.
/// </summary>
public abstract class ApiClientBase(HttpClient http, CliSession session)
{
    protected HttpClient Http { get; } = http;

    protected HttpRequestMessage CreateRequest(HttpMethod method, string requestUri, object? jsonBody = null)
    {
        if (string.IsNullOrWhiteSpace(session.ServerUrl))
            throw new InvalidOperationException("Not connected. Use 'login' to connect to a CatDb.Server first.");

        var baseUri = new Uri(session.ServerUrl.TrimEnd('/') + "/");
        var request = new HttpRequestMessage(method, new Uri(baseUri, requestUri.TrimStart('/')))
        {
            Headers = { Authorization = BuildAuthorizationHeader(session.UserName ?? "", session.Password ?? "") }
        };

        if (jsonBody is not null)
            request.Content = JsonContent.Create(jsonBody, options: JsonDefaults.Options);

        return request;
    }

    public static AuthenticationHeaderValue BuildAuthorizationHeader(string userName, string password)
    {
        var raw = Encoding.UTF8.GetBytes($"{userName}:{password}");
        return new AuthenticationHeaderValue("Basic", Convert.ToBase64String(raw));
    }

    protected async Task<T> SendAsync<T>(HttpRequestMessage request, CancellationToken ct = default)
    {
        using var response = await Http.SendAsync(request, ct).ConfigureAwait(false);
        await EnsureSuccessAsync(response, ct).ConfigureAwait(false);

        // Deserialize from a fully-buffered byte array rather than ReadFromJsonAsync's streaming
        // path: any JsonElement-typed property in T (KeyValueRow, TableInfo's KeySchema, …) would
        // otherwise reference the reader's pooled buffer, which is returned to the pool as soon as
        // the stream read completes — later access (e.g. GetRawText() when printing the result)
        // then throws "Operation is not valid due to the current state of the object."
        var bytes = await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
        var result = JsonSerializer.Deserialize<T>(bytes, JsonDefaults.Options);
        return result ?? throw new ApiException(response.StatusCode, "The server returned an empty response.");
    }

    protected async Task SendAsync(HttpRequestMessage request, CancellationToken ct = default)
    {
        using var response = await Http.SendAsync(request, ct).ConfigureAwait(false);
        await EnsureSuccessAsync(response, ct).ConfigureAwait(false);
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode)
            return;

        string message = response.ReasonPhrase ?? response.StatusCode.ToString();
        try
        {
            var bytes = await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
            var payload = JsonSerializer.Deserialize<JsonElement>(bytes, JsonDefaults.Options);
            if (payload.ValueKind == JsonValueKind.Object && payload.TryGetProperty("error", out var errorEl))
                message = errorEl.GetString() ?? message;
        }
        catch (JsonException)
        {
            // Response body wasn't a JSON error envelope; fall back to the reason phrase.
        }

        throw new ApiException(response.StatusCode, message);
    }
}
