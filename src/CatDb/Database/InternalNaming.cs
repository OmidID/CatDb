// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Database;

/// <summary>
/// Naming rules for engine-internal tables (secondary indexes, and any future
/// internal structure). Internal tables are backed by ordinary WTree locators but
/// are hidden from the public engine surface (enumeration / <c>Count</c> /
/// <c>Exists</c> / indexer / <c>Rename</c>) and users may not create tables whose
/// name collides with the reserved prefix.
/// </summary>
public static class InternalNaming
{
    /// <summary>
    /// Reserved name prefix. Any table whose name starts with this is internal and
    /// invisible to public callers. Index tables use it (see
    /// <see cref="Indexing.IndexDefinition.GetTableName"/>).
    /// </summary>
    public const string ReservedPrefix = "__";

    /// <summary>True when <paramref name="name"/> denotes an engine-internal table.</summary>
    public static bool IsInternal(string? name) =>
        name is not null && name.StartsWith(ReservedPrefix, StringComparison.Ordinal);
}
