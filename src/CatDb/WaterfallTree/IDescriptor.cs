// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;
using CatDb.General.Persist;

namespace CatDb.WaterfallTree;

public interface IDescriptor
{
    long    Id            { get; }
    string? Name          { get; }
    int    StructureType { get; }

    /// Describes the key type.
    DataType KeyDataType { get; }

    /// Describes the record type.
    DataType RecordDataType { get; }

    /// Can be anonymous or user type.
    Type? KeyType    { get; set; }

    /// Can be anonymous or user type.
    Type? RecordType { get; set; }

    IComparer<IData>?          KeyComparer           { get; set; }
    IEqualityComparer<IData>?  KeyEqualityComparer   { get; set; }
    IPersist<IData>?           KeyPersist            { get; set; }
    IPersist<IData>?           RecordPersist         { get; set; }
    IIndexerPersist<IData>?    KeyIndexerPersist     { get; set; }
    IIndexerPersist<IData>?    RecordIndexerPersist  { get; set; }

    DateTime CreateTime   { get; }
    DateTime ModifiedTime { get; }
    DateTime AccessTime   { get; }

    /// <summary>
    /// Maps key member names to their slot indices.
    /// Non-null when the table was opened with a concrete (non-anonymous) key type.
    /// Persisted with the locator so it survives restarts.
    /// </summary>
    IReadOnlyDictionary<string, int>? KeyMembers { get; }

    /// <summary>
    /// Maps record/value member names to their slot indices.
    /// Non-null when the table was opened with a concrete (non-anonymous) record type.
    /// Persisted with the locator so it survives restarts.
    /// </summary>
    IReadOnlyDictionary<string, int>? RecordMembers { get; }

    /// <summary>
    /// Recursive key member map — descends into nested Slots and collection element
    /// types so deeply-nested schemas keep their real field names. Null when the
    /// table was opened with an anonymous key type.
    /// </summary>
    MemberMap? KeyMemberMap { get; }

    /// <summary>
    /// Recursive record/value member map (see <see cref="KeyMemberMap"/>).
    /// </summary>
    MemberMap? RecordMemberMap { get; }

    byte[]? Tag { get; set; }
}