// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Database.Indexing;

/// <summary>
/// Describes a secondary index: its name, the slots it covers, and its type.
/// Returned from <see cref="ITableIndexManager.CreateIndex"/> and
/// <see cref="ITableIndexManager.ListIndexes"/>.
/// </summary>
public sealed class IndexDefinition
{
    /// <summary>The user-visible name of this index.</summary>
    public string Name { get; }

    /// <summary>The record slot indices covered by this index (in order).</summary>
    public int[] SlotIndices { get; }

    /// <summary>The corresponding member/property names (if known), in the same order as <see cref="SlotIndices"/>.</summary>
    public string[] MemberNames { get; }

    /// <summary>Whether this index enforces a uniqueness constraint.</summary>
    public IndexType Type { get; }

    public IndexDefinition(string name, int[] slotIndices, string[] memberNames, IndexType type)
    {
        Name = name;
        SlotIndices = slotIndices;
        MemberNames = memberNames;
        Type = type;
    }

    /// <summary>The backing index table name in the storage engine.</summary>
    internal string GetTableName(string mainTableName)
    {
        var suffix = Type == IndexType.Unique ? "_u" : "_nu";
        return $"{InternalNaming.ReservedPrefix}idx_{mainTableName}_{Name}{suffix}";
    }
}
