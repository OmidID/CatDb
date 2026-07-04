using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace CatDb.AdminUi.Infrastructure;

/// <summary>
/// Common request/response plumbing shared by every typed CatDb.Server API client: resolves the
/// request against the current <see cref="ApiConnectionState.BaseUrl"/> (user-configurable at
/// login, not fixed at DI-registration time), attaches the Basic-auth header for the signed-in
/// user, and turns non-success responses into a single <see cref="ApiException"/> type the UI can catch.
/// </summary>
public abstract class ApiClientBase(HttpClient http, ApiCredentialProvider credentials, ApiConnectionState connection)
{
    protected HttpClient Http { get; } = http;

    protected HttpRequestMessage CreateRequest(HttpMethod method, string requestUri, object? jsonBody = null)
    {
        var baseUri = new Uri(connection.BaseUrl.TrimEnd('/') + "/");
        var request = new HttpRequestMessage(method, new Uri(baseUri, requestUri.TrimStart('/')))
        {
            Headers = { Authorization = credentials.BuildAuthorizationHeader() }
        };

        if (jsonBody is not null)
            request.Content = JsonContent.Create(jsonBody, options: JsonDefaults.Options);

        return request;
    }

    protected async Task<T> SendAsync<T>(HttpRequestMessage request, CancellationToken ct = default)
    {
        using var response = await Http.SendAsync(request, ct).ConfigureAwait(false);
        await EnsureSuccessAsync(response, ct).ConfigureAwait(false);

        var result = await response.Content.ReadFromJsonAsync<T>(JsonDefaults.Options, ct).ConfigureAwait(false);
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
            var payload = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct).ConfigureAwait(false);
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
