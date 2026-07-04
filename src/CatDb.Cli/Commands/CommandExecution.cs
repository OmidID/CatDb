// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.CommandLine;
using CatDb.Cli.Api;
using CatDb.Cli.Output;

namespace CatDb.Cli.Commands;

/// <summary>
/// Wraps a command's action body with the one error-handling policy every command needs: an
/// <see cref="ApiException"/> (or anything else) is printed through <see cref="IOutputWriter"/> and
/// turned into exit code 1, instead of every one of the ~25 command classes repeating its own
/// try/catch. Exit codes follow Unix convention (0 success, 1 error, 130 Ctrl+C) so <c>-o</c>
/// one-shot invocations are script-friendly.
/// </summary>
public static class CommandExecution
{
    public static Func<ParseResult, CancellationToken, Task<int>> Wrap(
        IOutputWriter output, Func<ParseResult, CancellationToken, Task> body) =>
        async (parseResult, ct) =>
        {
            try
            {
                await body(parseResult, ct).ConfigureAwait(false);
                return 0;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return 130;
            }
            catch (ApiException ex)
            {
                output.WriteError(ex.Message);
                return 1;
            }
            catch (Exception ex)
            {
                // Command bodies are the CLI's outermost boundary — anything unhandled here (bad
                // --key/--value JSON, HTTP transport failures, etc.) must still exit(1) with a
                // message instead of an unhandled-exception stack trace.
                output.WriteError(ex.Message);
                return 1;
            }
        };
}
