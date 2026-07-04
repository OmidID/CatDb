// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Cli.Commands;

/// <summary>Shared "are you sure?" gate for destructive commands (db-delete, table-delete, …).
/// Every such command exposes a <c>--yes</c> option that maps to <paramref name="assumeYes"/>.</summary>
public static class Confirmation
{
    public static bool Confirm(string prompt, bool assumeYes)
    {
        if (assumeYes)
            return true;

        if (Console.IsInputRedirected)
            throw new InvalidOperationException($"{prompt} Re-run with --yes to confirm in a non-interactive session.");

        Console.Write($"{prompt} [y/N] ");
        var answer = Console.ReadLine();
        return answer is not null && answer.Trim().Equals("y", StringComparison.OrdinalIgnoreCase);
    }
}
