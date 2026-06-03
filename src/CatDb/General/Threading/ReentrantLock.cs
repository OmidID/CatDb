// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.General.Threading;

/// <summary>
/// A reentrant mutual-exclusion lock built on <see cref="Monitor"/>.
/// If the calling thread already holds the lock, <see cref="Enter"/> increments
/// the recursion depth and returns immediately — preventing self-deadlock.
/// Every <see cref="Enter"/> must be paired with exactly one <see cref="Exit"/>.
/// </summary>
public sealed class ReentrantLock
{
    private readonly object _sync = new();
    private Thread? _owner;
    private int _depth;

    /// <summary>
    /// Acquires the lock.  If the calling thread already holds it the call
    /// is a no-op (depth counter is incremented).
    /// </summary>
    public void Enter()
    {
        var current = Thread.CurrentThread;

        if (_owner == current)
        {
            _depth++;
            return;
        }

        Monitor.Enter(_sync);
        // Memory barrier from Monitor.Enter ensures _owner / _depth writes
        // done by the previous holder are visible here.
        _owner = current;
        _depth = 1;
    }

    /// <summary>
    /// Releases one level of recursion.  When the outermost Exit is called
    /// the lock is fully released and waiting threads may proceed.
    /// </summary>
    public void Exit()
    {
        if (--_depth == 0)
        {
            _owner = null;
            Monitor.Exit(_sync);
        }
    }

    /// <summary>True when the calling thread currently holds this lock.</summary>
    public bool IsHeldByCurrentThread => _owner == Thread.CurrentThread;

    /// <summary>
    /// Acquires the lock and returns a scope that releases it on Dispose.
    /// Enables <c>using (x.Lock()) { ... }</c> as a drop-in replacement for
    /// <c>lock (x) { ... }</c> — including early <c>return</c> inside the block.
    /// </summary>
    public Scope Lock()
    {
        Enter();
        return new Scope(this);
    }

    public readonly struct Scope : IDisposable
    {
        private readonly ReentrantLock _lock;
        internal Scope(ReentrantLock @lock) => _lock = @lock;
        public void Dispose() => _lock.Exit();
    }
}
