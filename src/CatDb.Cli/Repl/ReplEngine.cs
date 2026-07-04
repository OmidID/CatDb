// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.CommandLine;
using CatDb.Cli.Session;

namespace CatDb.Cli.Repl;

/// <summary>
/// psql-style interactive prompt: reads one line at a time, tokenizes it, and parses/invokes it
/// against the same <see cref="RootCommand"/> tree used for one-shot (<c>-o</c>) invocations, so
/// every command behaves identically in both modes. Ctrl+C cancels only the in-flight command, not
/// the whole session; <c>exit</c>/<c>quit</c> (or EOF/Ctrl+D) end the loop.
/// </summary>
public sealed class ReplEngine(RootCommand root, CliSession session)
{
    public async Task RunAsync(CancellationToken shutdownToken)
    {
        Console.WriteLine("CatDb interactive prompt. Type --help to list commands, 'exit' or 'quit' to leave.");

        while (!shutdownToken.IsCancellationRequested)
        {
            Console.Write(Prompt());
            var line = Console.ReadLine();
            if (line is null)
                break;

            line = line.Trim();
            if (line.Length == 0)
                continue;
            if (line is "exit" or "quit" or @"\q")
                break;

            await RunLineAsync(line, shutdownToken).ConfigureAwait(false);
        }
    }

    private async Task RunLineAsync(string line, CancellationToken shutdownToken)
    {
        using var lineCts = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);
        ConsoleCancelEventHandler onCancel = (_, e) =>
        {
            // Cancel just this command, not the process — keeps the prompt alive.
            e.Cancel = true;
            lineCts.Cancel();
        };

        Console.CancelKeyPress += onCancel;
        try
        {
            var parseResult = root.Parse(CommandLineTokenizer.Tokenize(line));
            await parseResult.InvokeAsync(new InvocationConfiguration(), lineCts.Token).ConfigureAwait(false);
        }
        finally
        {
            Console.CancelKeyPress -= onCancel;
        }
    }

    private string Prompt()
    {
        var host = session.ServerUrl is null ? "catdb" : new Uri(session.ServerUrl).Host;
        var db = session.CurrentDatabase is null ? "" : $" [{session.CurrentDatabase}]";
        return $"{host}{db}> ";
    }
}
