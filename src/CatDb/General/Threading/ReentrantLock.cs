// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.General.Threading;

/// <summary>
/// A reentrant mutual-exclusion lock built on <see cref="System.Threading.Lock"/> (.NET 9+).
/// If the calling thread already holds the lock, <see cref="Enter"/> increments the recursion depth and
/// returns immediately — preventing self-deadlock. Every <see cref="Enter"/> must be paired with one <see cref="Exit"/>.
/// <para>
/// A hand-rolled flag/CAS spinlock was tried (to shave a few ns on uncontended acquire) but REVERTED: it
/// produced an intermittent "No such handle" — a dangling node reference from a tree mutation racing a
/// concurrent one, i.e. a mutual-exclusion gap under the WTree's reentrant + hand-over-hand locking. The
/// runtime's <see cref="System.Threading.Lock"/> has correct fairness/barriers and (unlike Monitor) keeps
/// its state in dedicated fields, so it does not inflate per-object sync-block entries — flat under load.
/// </para>
/// </summary>
public sealed class ReentrantLock
{
    private readonly System.Threading.Lock _sync = new();
    private Thread? _owner;
    private int _depth;

    /// <summary>Acquires the lock. If the calling thread already holds it, increments the depth counter.</summary>
    public void Enter()
    {
        var current = Thread.CurrentThread;

        if (_owner == current)
        {
            _depth++;
            return;
        }

        _sync.Enter();
        // The acquire barrier from Lock.Enter ensures _owner / _depth writes
        // done by the previous holder are visible here.
        _owner = current;
        _depth = 1;
    }

    /// <summary>Releases one level of recursion; the outermost Exit fully releases the lock.</summary>
    public void Exit()
    {
        if (--_depth == 0)
        {
            _owner = null;
            _sync.Exit();
        }
    }

    /// <summary>True when the calling thread currently holds this lock.</summary>
    public bool IsHeldByCurrentThread => _owner == Thread.CurrentThread;

    /// <summary>
    /// Acquires the lock and returns a scope that releases it on Dispose.
    /// <c>using (x.Lock()) { ... }</c> — drop-in replacement for <c>lock (x) { ... }</c>.
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
