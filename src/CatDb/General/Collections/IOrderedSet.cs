// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.General.Threading;

namespace CatDb.General.Collections;
public interface IOrderedSet<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
{
    DataRwLock Lock { get; }

    IComparer<TKey> Comparer { get; }
    IEqualityComparer<TKey> EqualityComparer { get; }

    void Add(KeyValuePair<TKey, TValue> kv);
    void Add(TKey key, TValue value);

    void UnsafeAdd(TKey key, TValue value);

    bool Remove(TKey key);
    bool Remove(TKey from, bool hasFrom, TKey to, bool hasTo);

    bool ContainsKey(TKey key);
    bool TryGetValue(TKey key, out TValue value);

    TValue this[TKey key] { get; set; }

    void Clear();

    /// <summary>Releases over-allocated backing capacity. The List backing doubles and never shrinks, so
    /// after a big bulk-load/sink followed by a split or deletes the backing array stays huge and mostly
    /// empty — a &gt;85&#160;KB Large Object Heap object that wastes memory and churns the (non-compacting) LOH.
    /// No-op when not badly over-allocated.</summary>
    void TrimExcess();

    bool IsInternallyOrdered { get; }
    IEnumerable<KeyValuePair<TKey, TValue>> InternalEnumerate();

    IOrderedSet<TKey, TValue> Split(int count);
    void Merge(IOrderedSet<TKey, TValue> set);

    void LoadFrom(KeyValuePair<TKey, TValue>[] array, int count, bool isOrdered);

    /// <summary>Inclusive bounded forward iteration (existing contract).</summary>
    IEnumerable<KeyValuePair<TKey, TValue>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo);

    /// <summary>
    /// Forward iteration with optional exclusive endpoints.
    /// The binary-search start/stop positions are adjusted by one index position when
    /// the matching boundary key is found, so no per-record predicate call is needed.
    /// </summary>
    IEnumerable<KeyValuePair<TKey, TValue>> ForwardExclusive(
        TKey from, bool hasFrom, bool fromExclusive,
        TKey to,   bool hasTo,   bool toExclusive);

    /// <summary>Inclusive bounded backward iteration (existing contract).</summary>
    IEnumerable<KeyValuePair<TKey, TValue>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom);

    /// <summary>
    /// Backward iteration with optional exclusive endpoints.
    /// Mirrors <see cref="ForwardExclusive"/> for the descending direction.
    /// </summary>
    IEnumerable<KeyValuePair<TKey, TValue>> BackwardExclusive(
        TKey to,   bool hasTo,   bool toExclusive,
        TKey from, bool hasFrom, bool fromExclusive);

    // ─── Direct-access API for zero-yield leaf iteration ──────────────────

    /// <summary>
    /// Returns the internal sorted backing list for direct indexed access.
    /// Returns <c>null</c> when the ordered set is in dictionary or red-black-tree mode.
    /// <para>
    /// Callers MUST hold the appropriate lock while reading from the returned list.
    /// </para>
    /// </summary>
    List<KeyValuePair<TKey, TValue>>? InternalList => null;

    /// <summary>
    /// Computes start/end indices for a range inside <see cref="InternalList"/>
    /// with optional exclusive endpoints — using binary search and index arithmetic only.
    /// Returns <c>false</c> if not in sorted-list mode, the set is empty, or the range is empty.
    /// <para>
    /// Time: O(log n) via binary search — zero per-record work.
    /// When <c>true</c>, the caller can iterate <c>InternalList[startIndex..endIndex]</c> directly.
    /// </para>
    /// </summary>
    bool TryGetSortedRange(
        TKey from, bool hasFrom, bool fromExclusive,
        TKey to,   bool hasTo,   bool toExclusive,
        out int startIndex, out int endIndex)
    {
        startIndex = endIndex = 0;
        return false;
    }

    /// <summary>
    /// Returns the number of records in the given range, using index arithmetic only.
    /// Time: O(log n) for the binary search.  Zero per-record work.
    /// Returns <c>-1</c> if the fast path is unavailable (dictionary / tree mode);
    /// the caller should fall back to counting via iteration.
    /// </summary>
    int CountRange(
        TKey from, bool hasFrom, bool fromExclusive,
        TKey to,   bool hasTo,   bool toExclusive)
    {
        if (TryGetSortedRange(from, hasFrom, fromExclusive, to, hasTo, toExclusive,
                              out var si, out var ei))
            return ei - si + 1;
        return -1; // fast path not available
    }

    KeyValuePair<TKey, TValue> First { get; }
    KeyValuePair<TKey, TValue> Last { get; }

    int Count { get; }
}
