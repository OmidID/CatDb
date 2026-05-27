using System.Text;
using Microsoft.AspNetCore.Http;

namespace CatDb.Server.Services;

public static class BasicAuthHelpers
{
    public static bool TryReadCredentials(HttpContext context, out string userName, out string password)
    {
        userName = string.Empty;
        password = string.Empty;

        if (!context.Request.Headers.TryGetValue("Authorization", out var values))
            return false;

        var header = values.ToString();
        if (!header.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
            return false;

        var encoded = header["Basic ".Length..].Trim();

        string decoded;
        try
        {
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(encoded));
        }
        catch
        {
            return false;
        }

        var separator = decoded.IndexOf(':');
        if (separator <= 0)
            return false;

        userName = decoded[..separator];
        password = decoded[(separator + 1)..];
        return true;
    }
}
