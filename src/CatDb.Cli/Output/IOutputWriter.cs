// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Cli.Output;

/// <summary>
/// Every command prints through this instead of touching <see cref="Console"/> directly, so the same
/// command works both for a human at a REPL prompt (<see cref="OutputFormat.Text"/>) and for a script
/// consuming <c>-o</c> one-shot output (<see cref="OutputFormat.Json"/>).
/// </summary>
public interface IOutputWriter
{
    OutputFormat Format { get; set; }

    /// <summary>Prints one object: a property/value list in Text mode, a JSON object in Json mode.</summary>
    void WriteResult<T>(T value);

    /// <summary>Prints a collection: an aligned column table in Text mode, a JSON array in Json mode.</summary>
    void WriteRows<T>(IReadOnlyCollection<T> rows);

    /// <summary>A short human-readable confirmation, e.g. "Database 'x' created."</summary>
    void WriteMessage(string message);

    void WriteError(string message);
}
