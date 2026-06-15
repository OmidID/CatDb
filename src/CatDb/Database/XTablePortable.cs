// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
using System.Collections;
using CatDb.General.Threading;
using CatDb.Data;
using CatDb.Database.Indexing;
using CatDb.Database.Operations;
using CatDb.General.Collections;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class XTablePortable : ITable<IData, IData>
{
    private IOperationCollection _operations;
    private TableIndexManager? _indexManager;

    public readonly WTree    Tree;
    public readonly Locator  Locator;
    public volatile bool     IsModified;
    public readonly ReentrantLock SyncRoot = new();

    internal XTablePortable(WTree tree, Locator locator)
    {
        Tree    = tree;
        Locator = locator;
        // Larger default queue lowers Execute/Flush frequency under heavy write load.
        // 1024 still fits comfortably under root sink threshold (default 4096).
        _operations = locator.OperationCollectionFactory.Create(1024);
    }

    /// <summary>
    /// Secondary index manager. Lazily created on first access.
    /// </summary>
    public ITableIndexManager Indexes
    {
        get
        {
            if (_indexManager == null)
                _indexManager = new TableIndexManager(this);
            return _indexManager;
        }
    }

    private void Execute(IOperation operation)
    {
        SyncRoot.Enter();
        try
        {
            IsModified = true;

            if (_operations.Capacity == 0)
            {
                Tree.Execute(Locator, operation);
                return;
            }

            _operations.Add(operation);
            if (_operations.Count == _operations.Capacity)
                Flush();
        }
        finally { SyncRoot.Exit(); }
    }

    public void Flush()
    {
        SyncRoot.Enter();
        try
        {
            if (_operations.Count == 0 || Tree.IsDisposed)
                return;

            Tree.Execute(_operations);
            _operations.Clear();
        }
        finally { SyncRoot.Exit(); }
    }

    public IData this[IData key]
    {
        get
        {
            if (!TryGet(key, out var record))
                throw new KeyNotFoundException(key.ToString());
            return record;
        }
        set => Replace(key, value);
    }

    public void Replace(IData key, IData record)
    {
        if (_indexManager is { HasIndexes: true })
        {
            SyncRoot.Enter();
            try
            {
                Flush();
                IData? oldRecord = null;
                TryGetInternal(key, out oldRecord);
                Execute(new ReplaceOperation(key, record));
                _indexManager.OnReplace(key, record, oldRecord);
            }
            finally { SyncRoot.Exit(); }
        }
        else
        {
            Execute(new ReplaceOperation(key, record));
        }
    }

    public void InsertOrIgnore(IData key, IData record)
    {
        if (_indexManager is { HasIndexes: true })
        {
            SyncRoot.Enter();
            try
            {
                Flush();
                if (TryGetInternal(key, out _))
                    return; // Key exists — no-op
                Execute(new InsertOrIgnoreOperation(key, record));
                _indexManager.OnReplace(key, record, null);
            }
            finally { SyncRoot.Exit(); }
        }
        else
        {
            Execute(new InsertOrIgnoreOperation(key, record));
        }
    }

    public void Delete(IData key)
    {
        if (_indexManager is { HasIndexes: true })
        {
            SyncRoot.Enter();
            try
            {
                Flush();
                if (TryGetInternal(key, out var oldRecord))
                {
                    Execute(new DeleteOperation(key));
                    _indexManager.OnDelete(key, oldRecord);
                }
                else
                {
                    Execute(new DeleteOperation(key));
                }
            }
            finally { SyncRoot.Exit(); }
        }
        else
        {
            Execute(new DeleteOperation(key));
        }
    }

    public void Delete(IData fromKey, IData toKey)
    {
        if (_indexManager is { HasIndexes: true })
        {
            SyncRoot.Enter();
            try
            {
                Flush();
                // Materialize affected records before deleting
                var affected = Forward(fromKey, true, toKey, true).ToList();
                Execute(new DeleteRangeOperation(fromKey, toKey));
                foreach (var kv in affected)
                    _indexManager.OnDelete(kv.Key, kv.Value);
            }
            finally { SyncRoot.Exit(); }
        }
        else
        {
            Execute(new DeleteRangeOperation(fromKey, toKey));
        }
    }

    public void Clear()
    {
        if (_indexManager is { HasIndexes: true })
        {
            Execute(new ClearOperation());
            _indexManager.OnClear();
        }
        else
        {
            Execute(new ClearOperation());
        }
    }
    public bool Exists(IData key)                       => TryGet(key, out _);

    /// <summary>
    /// Internal TryGet that does NOT acquire SyncRoot (caller must hold it).
    /// Used by index maintenance to read old records without deadlock.
    /// </summary>
    private bool TryGetInternal(IData key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out IData? record)
    {
        var lastVisitedFullKey = default(WTree.FullKey);
        var records = Tree.FindData(Locator, Locator, key, Direction.Forward, out _, out _, ref lastVisitedFullKey);
        if (records is null)
        {
            record = default;
            return false;
        }

        records.Lock.EnterRead();
        try { return records.TryGetValue(key, out record); }
        finally { records.Lock.ExitRead(); }
    }

    public bool TryGet(IData key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out IData? record)
    {
        SyncRoot.Enter();
        try
        {
            Flush();

            var lastVisitedFullKey = default(WTree.FullKey);
            var records = Tree.FindData(Locator, Locator, key, Direction.Forward, out _, out _, ref lastVisitedFullKey);
            if (records is null)
            {
                record = default;
                return false;
            }

            records.Lock.EnterRead();
            try { return records.TryGetValue(key, out record); }
            finally { records.Lock.ExitRead(); }
        }
        finally { SyncRoot.Exit(); }
    }

    public IData Find(IData key)
    {
        TryGet(key, out var record);
        return record;
    }

    public IData TryGetOrDefault(IData key, IData defaultRecord) =>
        TryGet(key, out var record) ? record : defaultRecord;

    public KeyValuePair<IData, IData>? FindNext(IData key)
    {
        SyncRoot.Enter();
        try
        {
            foreach (var kv in Forward(key, true, default, false))
                return kv;
            return null;
        }
        finally { SyncRoot.Exit(); }
    }

    public KeyValuePair<IData, IData>? FindAfter(IData key)
    {
        SyncRoot.Enter();
        try
        {
            var comparer = Locator.KeyComparer;
            foreach (var kv in Forward(key, true, default, false))
            {
                if (comparer.Compare(kv.Key, key) != 0)
                    return kv;
            }
            return null;
        }
        finally { SyncRoot.Exit(); }
    }

    public KeyValuePair<IData, IData>? FindPrev(IData key)
    {
        SyncRoot.Enter();
        try
        {
            foreach (var kv in Backward(key, true, default, false))
                return kv;
            return null;
        }
        finally { SyncRoot.Exit(); }
    }

    public KeyValuePair<IData, IData>? FindBefore(IData key)
    {
        SyncRoot.Enter();
        try
        {
            var comparer = Locator.KeyComparer;
            foreach (var kv in Backward(key, true, default, false))
            {
                if (comparer.Compare(kv.Key, key) != 0)
                    return kv;
            }
            return null;
        }
        finally { SyncRoot.Exit(); }
    }

    public IEnumerable<KeyValuePair<IData, IData>> Forward() =>
        Forward(default, false, default, false);

    public IEnumerable<KeyValuePair<IData, IData>> Forward(IData from, bool hasFrom, IData to, bool hasTo)
    {
        SyncRoot.Enter();
        try
        {
            var keyComparer = Locator.KeyComparer;

            if (hasFrom && hasTo && keyComparer.Compare(from, to) > 0)
                throw new ArgumentException("from > to");

            Flush();

            var lastVisitedFullKey = default(WTree.FullKey);
            var records = Tree.FindData(Locator, Locator, hasFrom ? from : null, Direction.Forward, out var nearFullKey, out var hasNearFullKey, ref lastVisitedFullKey);

            if (records is null)
            {
                if (!hasNearFullKey || !nearFullKey.Locator.Equals(Locator))
                    yield break;

                records = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key, Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
            }

            // prevLastKey: maximum key of the most-recently yielded leaf. Symmetric to the Backward scan's
            // prevFirstKey guard. A Forward scan pages leaf-by-leaf, RELEASING the WTree root lock between
            // pages (FindData is hand-over-hand). Between pages a concurrent split/merge or a checkpoint
            // eviction+reload can land the next descent on a leaf whose first key is <= the last key we
            // already yielded — a backward jump. Unchecked, that yields out-of-order keys and, on an
            // unbounded scan, loops forever (re-reading the same range = a hang). The guard (checked under
            // the next page's read lock, atomic with the foreach) detects the regression and stops the scan
            // at a correctly-ordered prefix instead. (Forward previously lacked this; only Backward had it.)
            IData prevLastKey = null;

            while (records is not null)
            {
                IOrderedSet<IData, IData> recs = null;

                if (hasNearFullKey && nearFullKey.Locator.Equals(Locator))
                {
                    records.Lock.EnterRead();
                    try
                    {
                        if (hasTo && records.Count > 0 && keyComparer.Compare(records.First.Key, to) > 0)
                            break;
                    }
                    finally { records.Lock.ExitRead(); }
                }

                var guardFailed = false;
                records.Lock.EnterRead();
                try
                {
                    // A concurrent insert/split must not have placed a key <= prevLastKey into this page,
                    // which would regress the forward scan. Blocked from racing by our shared read lock.
                    if (prevLastKey != null && records.Count > 0 &&
                        keyComparer.Compare(records.First.Key, prevLastKey) <= 0)
                    {
                        guardFailed = true;
                    }
                    else
                    {
                        if (records.Count > 0)
                            prevLastKey = records.Last.Key;
                        foreach (var record in records.Forward(from, hasFrom, to, hasTo))
                            yield return record;
                    }
                }
                finally { records.Lock.ExitRead(); }

                // Sequential fetch of the next page — no background prefetch / Task.Wait.
                if (hasNearFullKey && nearFullKey.Locator.Equals(Locator))
                    recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key, Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);

                if (guardFailed) break;
                if (recs is null) break;
                if (ReferenceEquals(recs, records)) break;

                records = recs;
            }
        }
        finally { SyncRoot.Exit(); }
    }

    public IEnumerable<KeyValuePair<IData, IData>> Backward() =>
        Backward(default, false, default, false);

    public IEnumerable<KeyValuePair<IData, IData>> Backward(IData to, bool hasTo, IData from, bool hasFrom)
    {
        SyncRoot.Enter();
        try
        {
            var keyComparer = Locator.KeyComparer;

            if (hasFrom && hasTo && keyComparer.Compare(from, to) > 0)
                throw new ArgumentException("from > to");

            Flush();

            var lastVisitedFullKey = new WTree.FullKey(Locator, to);
            var records = Tree.FindData(Locator, Locator, hasTo ? to : null, Direction.Backward, out var nearFullKey, out var hasNearFullKey, ref lastVisitedFullKey);

            if (records is null)
                yield break;

            // prevFirstKey: minimum key of the most-recently yielded leaf.
            // The guard (checked under the next leaf's read lock) verifies that
            // no concurrent insert placed a key >= prevFirstKey into the next leaf
            // between navigation and iteration.  Check + buffer-fill are atomic
            // (same EnterRead block) so no write can slip in between.
            IData prevFirstKey = null;

            while (records is not null)
            {
                IOrderedSet<IData, IData> recs = null;

                if (hasNearFullKey)
                {
                    bool shouldStop;
                    records.Lock.EnterRead();
                    try { shouldStop = hasFrom && records.Count > 0 && keyComparer.Compare(records.Last.Key, from) < 0; }
                    finally { records.Lock.ExitRead(); }
                    if (shouldStop) break;
                }

                bool guardFailed = false;
                records.Lock.EnterRead();
                try
                {
                    // Guard: checked inside EnterRead so it is atomic with the foreach.
                    // A concurrent write (EnterWrite) is blocked by our shared lock,
                    // so no high key can be inserted between the check and the yields.
                    if (prevFirstKey != null && records.Count > 0 &&
                        keyComparer.Compare(records.Last.Key, prevFirstKey) >= 0)
                    {
                        guardFailed = true;
                    }
                    else
                    {
                        if (records.Count > 0)
                            prevFirstKey = records.First.Key;
                        foreach (var record in records.Backward(to, hasTo, from, hasFrom))
                            yield return record;
                    }
                }
                finally { records.Lock.ExitRead(); }

                // Sequential fetch of the next page — no background prefetch / Task.Wait.
                if (hasNearFullKey)
                    recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key, Direction.Backward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);

                if (guardFailed) break;
                if (recs is null) break;
                if (ReferenceEquals(recs, records)) break;

                records = recs;
            }
        }
        finally { SyncRoot.Exit(); }
    }

    public KeyValuePair<IData, IData>? FirstRow => Forward().Cast<KeyValuePair<IData, IData>?>().FirstOrDefault();
    public KeyValuePair<IData, IData>? LastRow  => Backward().Cast<KeyValuePair<IData, IData>?>().FirstOrDefault();
    public long Count()                        => this.LongCount();
    public IDescriptor Descriptor              => Locator;

    public IEnumerator<KeyValuePair<IData, IData>> GetEnumerator() => Forward().GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator()                        => GetEnumerator();

    public int OperationQueueCapacity
    {
        get
        {
            SyncRoot.Enter();
            try { return _operations.Capacity; }
            finally { SyncRoot.Exit(); }
        }
        set
        {
            SyncRoot.Enter();
            try
            {
                Flush();
                _operations = Locator.OperationCollectionFactory.Create(value);
            }
            finally { SyncRoot.Exit(); }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Zero-copy Count — O(leaves × log leafSize), no record access
    // ─────────────────────────────────────────────────────────────────────────
    //
    // For 2M records across ~62 leaves, this performs ~62 binary searches
    // instead of 2M record iterations + IData unwrapping. The speed
    // difference is ~1000x because we never touch the actual record data.
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Counts records matching the given range using leaf-level index arithmetic.
    /// No buffer copy, no IData unwrapping, no per-record work.
    /// Time: O(log N + leaves × log leafSize).
    /// </summary>
    public long ScanCount(
        IData from, bool hasFrom, bool fromExclusive,
        IData to,   bool hasTo,   bool toExclusive)
    {
        SyncRoot.Enter();
        try
        {
            var keyComparer = Locator.KeyComparer;

            if (hasFrom && hasTo)
            {
                var cmp = keyComparer.Compare(from, to);
                if (cmp > 0) return 0;
                if (cmp == 0 && (fromExclusive || toExclusive)) return 0;
            }

            Flush();

            long total = 0;

            var lastVisitedFullKey = default(WTree.FullKey);
            var records = Tree.FindData(Locator, Locator, hasFrom ? from : null,
                Direction.Forward, out var nearFullKey, out var hasNearFullKey, ref lastVisitedFullKey);

            if (records is null)
            {
                if (!hasNearFullKey || !nearFullKey.Locator.Equals(Locator))
                    return 0;
                records = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key,
                    Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
            }

            bool hasFromActive    = hasFrom;
            bool fromExclActive   = fromExclusive;
            // Forward paging regresses cursor under concurrent split/eviction (see Forward()): guard against
            // re-counting an earlier leaf / looping forever by stopping when the cursor stops advancing.
            IData prevScanLastKey = null;

            while (records is not null)
            {
                IOrderedSet<IData, IData> recs = null;

                if (hasNearFullKey && nearFullKey.Locator.Equals(Locator))
                {
                    bool shouldStop;
                    records.Lock.EnterRead();
                    try
                    {
                        shouldStop = hasTo && records.Count > 0 &&
                            keyComparer.Compare(records.First.Key, to) is var c &&
                            (c > 0 || (toExclusive && c == 0));
                    }
                    finally { records.Lock.ExitRead(); }
                    if (shouldStop) break;

                    recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key,
                        Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                }

                records.Lock.EnterRead();
                try
                {
                    if (prevScanLastKey != null && records.Count > 0 &&
                        keyComparer.Compare(records.First.Key, prevScanLastKey) <= 0)
                        break;  // cursor regressed — stop at a consistent prefix (finally releases the lock)
                    if (records.Count > 0)
                        prevScanLastKey = records.Last.Key;

                    // If neither from nor to applies to this leaf, the whole leaf counts.
                    // "from" only constrains the first leaf; "to" constrains the last.
                    // After the first leaf we clear hasFromActive, so only first leaf has it.
                    // If hasTo is false OR the leaf's last key < to, the whole leaf counts too.
                    bool needRange = hasFromActive || hasTo;

                    if (!needRange)
                    {
                        // Interior leaf — entire leaf is in range
                        total += records.Count;
                    }
                    else
                    {
                        // Boundary leaf — check if we can use shortcut
                        bool leafFullyInRange = true;

                        // Check if 'from' actually constrains this leaf
                        if (hasFromActive && records.Count > 0)
                        {
                            var cmpFirst = keyComparer.Compare(records.First.Key, from);
                            if (cmpFirst < 0 || (cmpFirst == 0 && fromExclActive))
                                leafFullyInRange = false; // from cuts into this leaf
                        }

                        // Check if 'to' actually constrains this leaf
                        if (hasTo && records.Count > 0)
                        {
                            var cmpLast = keyComparer.Compare(records.Last.Key, to);
                            if (cmpLast > 0 || (cmpLast == 0 && toExclusive))
                                leafFullyInRange = false; // to cuts into this leaf
                        }

                        if (leafFullyInRange)
                        {
                            total += records.Count;
                        }
                        else
                        {
                            // Boundary leaf needs range calculation
                            var leafCount = records.CountRange(
                                from, hasFromActive, fromExclActive,
                                to, hasTo, toExclusive);

                            if (leafCount >= 0)
                                total += leafCount;
                            else
                            {
                                // Non-list mode — iterate
                                foreach (var _ in records.ForwardExclusive(
                                             from, hasFromActive, fromExclActive,
                                             to, hasTo, toExclusive))
                                    total++;
                            }
                        }
                    }
                }
                finally { records.Lock.ExitRead(); }

                hasFromActive  = false;
                fromExclActive = false;

                if (ReferenceEquals(recs, records)) break;  // no progress — avoid spinning on the same leaf
                records = recs;
            }

            return total;
        }
        finally { SyncRoot.Exit(); }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Direct-callback scan — zero state machines, maximum throughput
    // ─────────────────────────────────────────────────────────────────────────
    //
    // For callers that can process records in bulk, this eliminates ALL
    // iterator state machines.  The callback receives the internal sorted
    // array segment directly — (array, startIndex, endIndex) — and runs
    // inside the leaf lock.
    //
    // Per-record cost: exactly the cost of the callback body.
    // No yield, no buffer copy, no state machine transitions.
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Callback signature for <see cref="ScanDirect"/>.
    /// Receives the leaf's internal backing array and the [start, end] inclusive indices.
    /// Return <c>false</c> to stop scanning (early termination); <c>true</c> to continue.
    /// <para>
    /// IMPORTANT: The callback runs inside <c>lock(records)</c>.
    /// Process the array segment quickly and do not call back into the WTree.
    /// </para>
    /// </summary>
    internal delegate bool ScanLeafCallback(
        List<KeyValuePair<IData, IData>> list, int startIndex, int endIndex);

    /// <summary>
    /// Forward scan with zero state machines and zero buffer copies.
    /// The callback receives each leaf's sorted backing list and matching index range,
    /// processes records via direct <c>list[i]</c> indexing, and returns <c>false</c>
    /// to stop.
    ///
    /// When a leaf is not in sorted-list mode (rare: dictionary / red-black tree),
    /// falls back to segment buffering for that leaf.
    /// </summary>
    internal void ScanDirect(
        IData from, bool hasFrom, bool fromExclusive,
        IData to,   bool hasTo,   bool toExclusive,
        ScanLeafCallback callback)
    {
        SyncRoot.Enter();
        try
        {
            var keyComparer = Locator.KeyComparer;

            if (hasFrom && hasTo)
            {
                var cmp = keyComparer.Compare(from, to);
                if (cmp > 0) return;
                if (cmp == 0 && (fromExclusive || toExclusive)) return;
            }

            Flush();

            var lastVisitedFullKey = default(WTree.FullKey);
            var records = Tree.FindData(Locator, Locator, hasFrom ? from : null,
                Direction.Forward, out var nearFullKey, out var hasNearFullKey, ref lastVisitedFullKey);

            if (records is null)
            {
                if (!hasNearFullKey || !nearFullKey.Locator.Equals(Locator))
                    return;
                records = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key,
                    Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
            }

            bool hasFromActive    = hasFrom;
            bool fromExclActive   = fromExclusive;
            // Same forward-paging regression guard as Forward()/ScanCount: stop if the cursor stops advancing.
            IData prevScanLastKey = null;

            while (records is not null)
            {
                IOrderedSet<IData, IData> recs = null;

                if (hasNearFullKey && nearFullKey.Locator.Equals(Locator))
                {
                    bool shouldStop;
                    records.Lock.EnterRead();
                    try
                    {
                        shouldStop = hasTo && records.Count > 0 &&
                            keyComparer.Compare(records.First.Key, to) is var c &&
                            (c > 0 || (toExclusive && c == 0));
                    }
                    finally { records.Lock.ExitRead(); }
                    if (shouldStop) break;

                    recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key,
                        Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                }

                bool continueScanning;
                records.Lock.EnterRead();
                try
                {
                    if (prevScanLastKey != null && records.Count > 0 &&
                        keyComparer.Compare(records.First.Key, prevScanLastKey) <= 0)
                    {
                        continueScanning = false;  // cursor regressed — stop at a consistent prefix
                    }
                    else
                    {
                        if (records.Count > 0)
                            prevScanLastKey = records.Last.Key;  // advance the regression cursor

                        if (records.TryGetSortedRange(
                                from, hasFromActive, fromExclActive,
                                to, hasTo, toExclusive,
                                out var si, out var ei))
                        {
                            // Zero-copy fast path: callback reads the internal list directly
                            continueScanning = callback(records.InternalList!, si, ei);
                        }
                        else if (records.InternalList == null && records.Count > 0)
                        {
                            // Slow path: materialize to temp list for non-list-mode leaves
                            var temp = new List<KeyValuePair<IData, IData>>();
                            foreach (var kv in records.ForwardExclusive(
                                         from, hasFromActive, fromExclActive,
                                         to, hasTo, toExclusive))
                                temp.Add(kv);
                            continueScanning = temp.Count == 0 || callback(temp, 0, temp.Count - 1);
                        }
                        else
                        {
                            continueScanning = true;
                        }
                    }
                }
                finally { records.Lock.ExitRead(); }

                if (!continueScanning) break;

                hasFromActive  = false;
                fromExclActive = false;

                if (ReferenceEquals(recs, records)) break;  // no progress — avoid spinning on the same leaf
                records = recs;
            }
        }
        finally { SyncRoot.Exit(); }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Segment-based scan — balanced throughput with composable IEnumerable API
    // ─────────────────────────────────────────────────────────────────────────
    //
    // Architecture:
    //   Previous approach: 3 nested C# iterator state machines per record
    //     ForwardExclusive → yield → XTablePortable.Scan → yield → XTable.Scan → yield → caller
    //   Each MoveNext() from the caller triggers 3 state machine transitions.
    //
    //   New approach: 1 state machine transition per record + 1 per leaf
    //     ScanSegments yields one buffer per LEAF (not per record).
    //     XTable.Scan does a tight for(i) loop over the buffer with direct array
    //     indexing, then yields to the caller — 1 state machine hop per record.
    //
    //   Additional wins:
    //     • lock(records) is held only during the buffer copy (short, deterministic time)
    //       instead of being held during the entire caller processing
    //     • direct List<T>.CopyTo → internal Array.Copy for the common list-mode path
    //     • the reusable buffer avoids per-leaf GC allocations
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Compact struct returned once per leaf by <see cref="ScanSegments"/>
    /// and <see cref="ScanSegmentsBackward"/>.
    /// <c>Buffer[0..Count-1]</c> contains the matching records for one leaf.
    /// The buffer is reused across leaves — the caller must finish processing
    /// before calling <c>MoveNext()</c>.
    /// </summary>
    internal readonly struct ScanSegment(KeyValuePair<IData, IData>[] buffer, int count)
    {
        public readonly KeyValuePair<IData, IData>[] Buffer = buffer;
        public readonly int Count = count;
    }

    /// <summary>
    /// Forward segment-based scan.  Yields one <see cref="ScanSegment"/> per leaf.
    ///
    /// Each segment contains the matching records buffered into a contiguous array.
    /// The records are copied out of the leaf inside <c>lock(records)</c>, then the
    /// lock is released before yielding — the caller's per-record processing happens
    /// without holding the leaf lock.
    /// </summary>
    internal IEnumerable<ScanSegment> ScanSegments(
        IData from, bool hasFrom, bool fromExclusive,
        IData to,   bool hasTo,   bool toExclusive,
        int maxRows = int.MaxValue)
    {
#if PERFORMANCE_CHECK
        var perfStart = System.Diagnostics.Stopwatch.GetTimestamp();
        long leafCount = 0;
        long segmentCount = 0;
        long rowCount = 0;
#endif

#if PERFORMANCE_CHECK
        var lockStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif
        SyncRoot.Enter();
#if PERFORMANCE_CHECK
        global::CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("xtable.scan.lockwait", lockStart);
#endif
        try
        {
            var keyComparer = Locator.KeyComparer;

            if (hasFrom && hasTo)
            {
                var cmp = keyComparer.Compare(from, to);
                if (cmp > 0) yield break;
                if (cmp == 0 && (fromExclusive || toExclusive)) yield break;
            }

#if PERFORMANCE_CHECK
            var flushStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif
            Flush();
#if PERFORMANCE_CHECK
            global::CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("xtable.scan.flush", flushStart);
#endif

            if (maxRows <= 0)
                yield break;

            var remaining = maxRows;

            var buffer = Array.Empty<KeyValuePair<IData, IData>>();

            var lastVisitedFullKey = default(WTree.FullKey);
            var records = Tree.FindData(Locator, Locator, hasFrom ? from : null,
                Direction.Forward, out var nearFullKey, out var hasNearFullKey, ref lastVisitedFullKey);

            if (records is null)
            {
                if (!hasNearFullKey || !nearFullKey.Locator.Equals(Locator))
                    yield break;
                records = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key,
                    Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
            }

            bool hasFromActive    = hasFrom;
            bool fromExclActive   = fromExclusive;
            IData fromActive      = from;
            IData prevSegLastKey  = null;  // forward-paging regression guard (see Forward())

            while (records is not null)
            {
#if PERFORMANCE_CHECK
                leafCount++;
#endif
                if (remaining <= 0)
                    break;

                IOrderedSet<IData, IData> recs = null;

                if (hasNearFullKey && nearFullKey.Locator.Equals(Locator))
                {
                    bool shouldStop;
                    records.Lock.EnterRead();
                    try
                    {
                        shouldStop = hasTo && records.Count > 0 &&
                            keyComparer.Compare(records.First.Key, to) is var c &&
                            (c > 0 || (toExclusive && c == 0));
                    }
                    finally { records.Lock.ExitRead(); }
                    if (shouldStop) break;

                    recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key,
                        Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                }

                int segCount = 0;
                var guardFailed = false;
                records.Lock.EnterRead();
                try
                {
                    if (prevSegLastKey != null && records.Count > 0 &&
                        keyComparer.Compare(records.First.Key, prevSegLastKey) <= 0)
                    {
                        guardFailed = true;  // cursor regressed — stop at a consistent prefix
                    }
                    else
                    {
                        if (records.Count > 0)
                            prevSegLastKey = records.Last.Key;

                        // ── Fast path: direct list access + Array.Copy ────────────────
                        if (records.TryGetSortedRange(
                                fromActive, hasFromActive, fromExclActive,
                                to, hasTo, toExclusive,
                                out var si, out var ei))
                        {
                            segCount = ei - si + 1;
                            if (segCount > remaining)
                                segCount = remaining;
                            if (buffer.Length < segCount)
                                buffer = new KeyValuePair<IData, IData>[Math.Max(segCount, 4096)];
                            records.InternalList!.CopyTo(si, buffer, 0, segCount);
                        }
                        else if (records.InternalList == null && records.Count > 0)
                        {
                            // ── Slow path: SortedSet / dictionary mode — materialize ──
                            var temp = new List<KeyValuePair<IData, IData>>();
                            foreach (var kv in records.ForwardExclusive(
                                         fromActive, hasFromActive, fromExclActive,
                                         to, hasTo, toExclusive))
                            {
                                temp.Add(kv);
                                if (temp.Count >= remaining)
                                    break;
                            }

                            segCount = temp.Count;
                            if (segCount > 0)
                            {
                                if (buffer.Length < segCount)
                                    buffer = new KeyValuePair<IData, IData>[Math.Max(segCount, 4096)];
                                temp.CopyTo(buffer);
                            }
                        }
                    }
                }
                finally { records.Lock.ExitRead(); }

                if (guardFailed) break;

                if (segCount > 0)
                {
#if PERFORMANCE_CHECK
                    segmentCount++;
                    rowCount += segCount;
#endif
                    remaining -= segCount;
                    yield return new ScanSegment(buffer, segCount);
                }

                hasFromActive  = false;
                fromExclActive = false;

                if (ReferenceEquals(recs, records)) break;  // no progress — avoid spinning on the same leaf
                records = recs;
            }
        }
        finally
        {
            SyncRoot.Exit();
#if PERFORMANCE_CHECK
            global::CatDb.General.Diagnostics.PerformanceCheck.Observe("xtable.scan.forward.leaves", leafCount);
            global::CatDb.General.Diagnostics.PerformanceCheck.Observe("xtable.scan.forward.segments", segmentCount);
            global::CatDb.General.Diagnostics.PerformanceCheck.Observe("xtable.scan.forward.rows", rowCount);
            global::CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("xtable.scan.forward", perfStart);
#endif
        }
    }

    /// <summary>
    /// Backward segment-based scan.  Yields one <see cref="ScanSegment"/> per leaf.
    ///
    /// <c>Buffer[0..Count-1]</c> contains the matching records in <b>descending</b>
    /// key order (reversed during the copy so the caller iterates 0 → Count-1 in
    /// natural descending order without a reverse loop).
    /// </summary>
    internal IEnumerable<ScanSegment> ScanSegmentsBackward(
        IData to,   bool hasTo,   bool toExclusive,
        IData from, bool hasFrom, bool fromExclusive,
        int maxRows = int.MaxValue)
    {
#if PERFORMANCE_CHECK
        var perfStart = System.Diagnostics.Stopwatch.GetTimestamp();
        long leafCount = 0;
        long segmentCount = 0;
        long rowCount = 0;
#endif

#if PERFORMANCE_CHECK
        var lockStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif
        SyncRoot.Enter();
#if PERFORMANCE_CHECK
        global::CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("xtable.scan.lockwait", lockStart);
#endif
        try
        {
            var keyComparer = Locator.KeyComparer;

            if (hasFrom && hasTo)
            {
                var cmp = keyComparer.Compare(from, to);
                if (cmp > 0) yield break;
                if (cmp == 0 && (fromExclusive || toExclusive)) yield break;
            }

#if PERFORMANCE_CHECK
            var flushStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif
            Flush();
#if PERFORMANCE_CHECK
            global::CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("xtable.scan.flush", flushStart);
#endif

            if (maxRows <= 0)
                yield break;

            var remaining = maxRows;

            var buffer = Array.Empty<KeyValuePair<IData, IData>>();

            var lastVisitedFullKey = new WTree.FullKey(Locator, to);
            var records = Tree.FindData(Locator, Locator, hasTo ? to : null,
                Direction.Backward, out var nearFullKey, out var hasNearFullKey, ref lastVisitedFullKey);

            if (records is null)
                yield break;

            bool hasToActive    = hasTo;
            bool toExclActive   = toExclusive;
            IData toActive      = to;

            // prevFirstKey: minimum key of the most-recently yielded leaf.
            // The guard (checked under the next leaf's read lock) verifies that
            // no concurrent insert placed a key >= prevFirstKey into the next leaf
            // between navigation and iteration.  Check + buffer-fill are atomic
            // (same EnterRead block) so no write can slip in between.
            IData prevFirstKey = null;

            while (records is not null)
            {
#if PERFORMANCE_CHECK
                leafCount++;
#endif
                if (remaining <= 0)
                    break;

                IOrderedSet<IData, IData> recs = null;

                if (hasNearFullKey)
                {
                    bool shouldStop;
                    records.Lock.EnterRead();
                    try
                    {
                        shouldStop = hasFrom && records.Count > 0 &&
                            keyComparer.Compare(records.Last.Key, from) is var c &&
                            (c < 0 || (fromExclusive && c == 0));
                    }
                    finally { records.Lock.ExitRead(); }
                    if (shouldStop) break;

                    recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key,
                        Direction.Backward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                }

                bool guardFailed = false;
                IData thisFirstKey = null;
                int segCount = 0;
                records.Lock.EnterRead();
                try
                {
                    // Guard: checked inside EnterRead so it is atomic with the buffer fill.
                    // A concurrent write (EnterWrite) is blocked by our shared lock,
                    // so no high key can be inserted between the check and the copy.
                    if (prevFirstKey != null && records.Count > 0 &&
                        keyComparer.Compare(records.Last.Key, prevFirstKey) >= 0)
                    {
                        guardFailed = true;
                    }
                    else
                    {
                        // ── Fast path: direct list access, reversed copy ──────────────
                        if (records.TryGetSortedRange(
                                from, hasFrom, fromExclusive,
                                toActive, hasToActive, toExclActive,
                                out var si, out var ei))
                        {
                            segCount = ei - si + 1;
                            if (segCount > remaining)
                            {
                                si = ei - remaining + 1;
                                segCount = remaining;
                            }
                            if (buffer.Length < segCount)
                                buffer = new KeyValuePair<IData, IData>[Math.Max(segCount, 4096)];
                            var list = records.InternalList!;
                            for (var i = 0; i < segCount; i++)
                                buffer[i] = list[ei - i]; // reverse into descending order

                            // Defensive: if the source list had any inversion the reversed
                            // buffer may not be in descending order.  Detect in O(n) and
                            // sort in O(n log n) only when needed (should be rare once all
                            // inversion-creation paths are fixed, but guards against any
                            // surviving corruption that reaches here).
                            if (segCount > 1)
                            {
                                bool inverted = false;
                                for (var k = 1; k < segCount; k++)
                                {
                                    if (keyComparer.Compare(buffer[k].Key, buffer[k - 1].Key) > 0)
                                    {
                                        inverted = true;
                                        break;
                                    }
                                }
                                if (inverted)
                                    Array.Sort(buffer, 0, segCount,
                                        System.Collections.Generic.Comparer<KeyValuePair<IData, IData>>.Create(
                                            (a, b) => keyComparer.Compare(b.Key, a.Key)));
                            }

                            // Use the actual minimum from the sorted buffer as thisFirstKey.
                            // records.First.Key == List[0] could be a large inversion-head key;
                            // buffer[segCount-1] is always the true minimum after the sort above.
                            if (segCount > 0)
                                thisFirstKey = buffer[segCount - 1].Key;
                        }
                        else if (records.InternalList == null && records.Count > 0)
                        {
                            // ── Slow path: SortedSet / dictionary — materialize reversed
                            var temp = new List<KeyValuePair<IData, IData>>();
                            foreach (var kv in records.BackwardExclusive(
                                         toActive, hasToActive, toExclActive,
                                         from, hasFrom, fromExclusive))
                            {
                                temp.Add(kv);
                                if (temp.Count >= remaining)
                                    break;
                            }

                            segCount = temp.Count;
                            if (segCount > 0)
                            {
                                if (buffer.Length < segCount)
                                    buffer = new KeyValuePair<IData, IData>[Math.Max(segCount, 4096)];
                                temp.CopyTo(buffer);
                                thisFirstKey = buffer[segCount - 1].Key;
                            }
                        }
                    }
                }
                finally { records.Lock.ExitRead(); }

                if (guardFailed) break;

                if (segCount > 0)
                {
#if PERFORMANCE_CHECK
                    segmentCount++;
                    rowCount += segCount;
#endif
                    remaining -= segCount;
                    yield return new ScanSegment(buffer, segCount);
                }

                // Only update prevFirstKey when this leaf had actual data.
                // An empty leaf (after range-delete) sets thisFirstKey = null; preserving
                // the previous value keeps the guard active across empty leaves so that
                // a subsequent wrong-direction leaf is still caught.
                if (thisFirstKey != null)
                    prevFirstKey = thisFirstKey;
                hasToActive  = false;
                toExclActive = false;

                if (recs is null) break;
                if (ReferenceEquals(recs, records)) break;

                records = recs;
            }
        }
        finally
        {
            SyncRoot.Exit();
#if PERFORMANCE_CHECK
            global::CatDb.General.Diagnostics.PerformanceCheck.Observe("xtable.scan.backward.leaves", leafCount);
            global::CatDb.General.Diagnostics.PerformanceCheck.Observe("xtable.scan.backward.segments", segmentCount);
            global::CatDb.General.Diagnostics.PerformanceCheck.Observe("xtable.scan.backward.rows", rowCount);
            global::CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("xtable.scan.backward", perfStart);
#endif
        }
    }

    // ── Legacy Scan / ScanBackward kept for ITable<IData,IData> callers ───

    public IEnumerable<KeyValuePair<IData, IData>> Scan(
        IData from, bool hasFrom, bool fromExclusive,
        IData to,   bool hasTo,   bool toExclusive)
    {
        foreach (var seg in ScanSegments(from, hasFrom, fromExclusive, to, hasTo, toExclusive))
        {
            var buf = seg.Buffer;
            var cnt = seg.Count;
            for (var i = 0; i < cnt; i++)
                yield return buf[i];
        }
    }

    public IEnumerable<KeyValuePair<IData, IData>> ScanBackward(
        IData to,   bool hasTo,   bool toExclusive,
        IData from, bool hasFrom, bool fromExclusive)
    {
        foreach (var seg in ScanSegmentsBackward(to, hasTo, toExclusive, from, hasFrom, fromExclusive))
        {
            var buf = seg.Buffer;
            var cnt = seg.Count;
            for (var i = 0; i < cnt; i++)
                yield return buf[i];
        }
    }
}
