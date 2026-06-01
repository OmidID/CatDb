// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.General.Threading;

/// <summary>
/// Non-reentrant reader-writer lock backed by <see cref="ReaderWriterLockSlim"/>.
///
/// Contract:
///   • Multiple threads may hold read mode concurrently — concurrent scans/lookups never block each other.
///   • Write mode is exclusive — an Apply/Split/Merge blocks until all readers finish, and vice-versa.
///   • Non-reentrant (NoRecursion policy) for maximum throughput — do not call Enter* while already
///     holding any mode on the same instance from the same thread.
///
/// This replaces the previous exclusive ReentrantLock on every IOrderedSet, which serialised all
/// concurrent scan operations regardless of whether a write was actually occurring.
/// Under read-heavy workloads the throughput difference is proportional to the number of reader threads.
/// </summary>
public sealed class DataRwLock
{
    private readonly ReaderWriterLockSlim _rwl = new(LockRecursionPolicy.NoRecursion);

    /// <summary>Enter shared (read) mode. Multiple threads may hold concurrently.</summary>
    public void EnterRead()  => _rwl.EnterReadLock();

    /// <summary>Exit shared (read) mode.</summary>
    public void ExitRead()   => _rwl.ExitReadLock();

    /// <summary>Enter exclusive (write) mode. Blocks until all readers and any writer exit.</summary>
    public void EnterWrite() => _rwl.EnterWriteLock();

    /// <summary>Exit exclusive (write) mode.</summary>
    public void ExitWrite()  => _rwl.ExitWriteLock();

    public bool IsWriteLockHeld => _rwl.IsWriteLockHeld;
    public bool IsReadLockHeld  => _rwl.IsReadLockHeld;
}
