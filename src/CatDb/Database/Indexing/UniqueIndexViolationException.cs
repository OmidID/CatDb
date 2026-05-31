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
