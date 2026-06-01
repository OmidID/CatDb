// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Database.Indexing;

/// <summary>
/// Defines the uniqueness constraint for a secondary index.
/// </summary>
public enum IndexType
{
    /// <summary>
    /// Multiple records may share the same indexed field value.
    /// Internally stored as a composite key (FieldValue, PrimaryKey) in the index WTree.
    /// </summary>
    NonUnique = 0,

    /// <summary>
    /// Each indexed field value must map to exactly one primary key.
    /// Violations throw <see cref="UniqueIndexViolationException"/>.
    /// Internally stored as a direct mapping (FieldValue → PrimaryKey) in the index WTree.
    /// </summary>
    Unique = 1,
}
