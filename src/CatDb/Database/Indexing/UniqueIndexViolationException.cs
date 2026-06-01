// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Database.Indexing;

/// <summary>
/// Thrown when an insert or update would violate a unique index constraint.
/// </summary>
public sealed class UniqueIndexViolationException : InvalidOperationException
{
    public string IndexName { get; }
    public object? FieldValue { get; }
    public object? ExistingKey { get; }

    public UniqueIndexViolationException(string indexName, object? fieldValue, object? existingKey)
        : base($"Unique index '{indexName}' violation: value '{fieldValue}' already exists for key '{existingKey}'.")
    {
        IndexName = indexName;
        FieldValue = fieldValue;
        ExistingKey = existingKey;
    }
}
