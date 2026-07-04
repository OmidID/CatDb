// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Cli.Api;

namespace CatDb.Cli.Session;

/// <summary>
/// Fills in whatever <see cref="CliSession"/> is missing, in this priority order: value already on
/// the session (from CLI options) → environment variable → interactive prompt. The password is
/// never accepted as a bare CLI option value without being echoed back — if it is missing it is
/// always read with <see cref="MaskedConsole.ReadPassword"/>, so it never lingers in shell history
/// or `ps` output.
/// </summary>
public sealed class CredentialResolver(IAuthClient authClient)
{
    public const string ServerEnvVar = "CATDB_SERVER";
    public const string UserEnvVar = "CATDB_USER";
    public const string PasswordEnvVar = "CATDB_PASSWORD";

    /// <summary>Prompts for anything missing on <paramref name="session"/> and validates the result
    /// against the server. Returns false (with a message already printed) if the credentials are rejected.</summary>
    public async Task<bool> ResolveAndValidateAsync(CliSession session, CancellationToken ct = default)
    {
        session.ServerUrl ??= Environment.GetEnvironmentVariable(ServerEnvVar);
        if (string.IsNullOrWhiteSpace(session.ServerUrl))
        {
            Console.Write("Server address (e.g. http://localhost:5000): ");
            session.ServerUrl = Console.ReadLine()?.Trim();
        }

        if (string.IsNullOrWhiteSpace(session.ServerUrl))
        {
            Console.Error.WriteLine("A server address is required.");
            return false;
        }

        session.UserName ??= Environment.GetEnvironmentVariable(UserEnvVar);
        if (string.IsNullOrWhiteSpace(session.UserName))
        {
            Console.Write("Username: ");
            session.UserName = Console.ReadLine()?.Trim();
        }

        session.Password ??= Environment.GetEnvironmentVariable(PasswordEnvVar);
        if (session.Password is null)
            session.Password = MaskedConsole.ReadPassword("Password: ");

        if (string.IsNullOrWhiteSpace(session.UserName) || session.Password is null)
        {
            Console.Error.WriteLine("A username and password are required.");
            return false;
        }

        var valid = await authClient.ValidateCredentialsAsync(session, ct).ConfigureAwait(false);
        if (!valid)
        {
            Console.Error.WriteLine("Login failed: invalid username or password.");
            return false;
        }

        return true;
    }
}

/// <summary>Reads a line from the console without echoing typed characters.</summary>
public static class MaskedConsole
{
    public static string ReadPassword(string prompt)
    {
        Console.Write(prompt);

        if (Console.IsInputRedirected)
            return Console.ReadLine() ?? string.Empty;

        var buffer = new System.Text.StringBuilder();
        ConsoleKeyInfo key;
        while ((key = Console.ReadKey(intercept: true)).Key != ConsoleKey.Enter)
        {
            if (key.Key == ConsoleKey.Backspace)
            {
                if (buffer.Length > 0)
                {
                    buffer.Length--;
                    Console.Write("\b \b");
                }
                continue;
            }

            if (!char.IsControl(key.KeyChar))
            {
                buffer.Append(key.KeyChar);
                Console.Write('*');
            }
        }

        Console.WriteLine();
        return buffer.ToString();
    }
}
