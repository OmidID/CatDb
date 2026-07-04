// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.CommandLine;
using CatDb.Cli.Output;

namespace CatDb.Cli.Commands;

/// <summary>Clears the terminal. Command listing/help is System.CommandLine's own built-in
/// <c>--help</c>/<c>-h</c> on the root command — see <see cref="Repl.ReplEngine"/>'s banner.</summary>
public sealed class ClearCommand(IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var command = new Command("clear", "Clear the terminal screen.");
        command.SetAction(CommandExecution.Wrap(output, (_, _) =>
        {
            Console.Clear();
            return Task.CompletedTask;
        }));
        return command;
    }
}

/// <summary>Registered mainly so it shows up in <c>--help</c> and is a harmless no-op if ever run via
/// <c>-o exit</c>; <see cref="Repl.ReplEngine"/> actually leaves the loop by recognizing "exit"/"quit"
/// before parsing, so a REPL session doesn't run this action to quit.</summary>
public sealed class ExitCommand(IOutputWriter output) : ICliCommand
{
    public Command BuildCommand()
    {
        var command = new Command("exit", "Exit the interactive prompt.");
        command.Aliases.Add("quit");
        command.SetAction(CommandExecution.Wrap(output, (_, _) => Task.CompletedTask));
        return command;
    }
}
