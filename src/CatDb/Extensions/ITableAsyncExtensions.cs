using CatDb.Database;

namespace CatDb.Extensions;

public static class TableAsyncExtensions
{
    public static Task ReplaceAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, TRecord record, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.Replace(key, record),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task InsertOrIgnoreAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, TRecord record, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.InsertOrIgnore(key, record),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task DeleteAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.Delete(key),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task DeleteAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey fromKey, TKey toKey, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.Delete(fromKey, toKey),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task ClearAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            table.Clear,
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<bool> ExistsAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.Exists(key),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<TRecord?> TryGetAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.TryGet(key, out var value) ? value : default,
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<TRecord> FindAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.Find(key),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<TRecord> TryGetOrDefaultAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, TRecord defaultRecord, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.TryGetOrDefault(key, defaultRecord),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<KeyValuePair<TKey, TRecord>?> FindNextAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.FindNext(key),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<KeyValuePair<TKey, TRecord>?> FindAfterAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.FindAfter(key),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<KeyValuePair<TKey, TRecord>?> FindPrevAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.FindPrev(key),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<KeyValuePair<TKey, TRecord>?> FindBeforeAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey key, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.FindBefore(key),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<IEnumerable<KeyValuePair<TKey, TRecord>>> ForwardAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            table.Forward,
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<IEnumerable<KeyValuePair<TKey, TRecord>>> ForwardAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey from, bool hasFrom, TKey to, bool hasTo, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.Forward(from, hasFrom, to, hasTo),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<IEnumerable<KeyValuePair<TKey, TRecord>>> BackwardAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            table.Backward,
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<IEnumerable<KeyValuePair<TKey, TRecord>>> BackwardAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, TKey to, bool hasTo, TKey from, bool hasFrom, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            () => table.Backward(from, hasFrom, to, hasTo),
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }

    public static Task<long> CountAsync<TKey, TRecord>(this ITable<TKey, TRecord> table, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(
            table.Count,
            cancellationToken,
            TaskCreationOptions.None,
            TaskScheduler.Default);
    }
}