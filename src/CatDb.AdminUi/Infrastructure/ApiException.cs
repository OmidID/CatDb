using System.Net;

namespace CatDb.AdminUi.Infrastructure;

/// <summary>Thrown by API clients when the CatDb.Server HTTP API returns a non-success status code.</summary>
public sealed class ApiException(HttpStatusCode statusCode, string message) : Exception(message)
{
    public HttpStatusCode StatusCode { get; } = statusCode;
}
