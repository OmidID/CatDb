// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using System.Collections;
using System.Diagnostics;
using CatDb.General.Comparers;
using CatDb.General.Extensions;
using CatDb.General.Threading;

#pragma warning disable CS8602, CS8604, CS8600, CS8601, CS8603
namespace CatDb.General.Collections;
public class OrderedSet<TKey, TValue> : IOrderedSet<TKey, TValue>
    where TKey : notnull
{
    public DataRwLock Lock { get; } = new();
    protected List<KeyValuePair<TKey, TValue>>? List;
    // volatile: TransformDictionaryToTree() writes these under EnterRead() (concurrent readers).
    // On weakly-ordered CPUs (ARM/Apple Silicon) a plain store can become visible out-of-order;
    // volatile ensures the _set = newSet release-store is seen by any thread whose acquire-load
    // of _dictionary observes the null sentinel.
    private volatile Dictionary<TKey, TValue>? _dictionary;
    private volatile SortedSet<KeyValuePair<TKey, TValue>>? _set;

    private readonly IComparer<TKey> _comparer;
    private readonly IEqualityComparer<TKey> _equalityComparer;
    protected KeyValuePairComparer<TKey, TValue> KvComparer = null!;

    protected OrderedSet(IComparer<TKey> comparer, IEqualityComparer<TKey> equalityComparer, List<KeyValuePair<TKey, TValue>> list)
    {
        _comparer = comparer;
        _equalityComparer = equalityComparer;
        KvComparer = new KeyValuePairComparer<TKey, TValue>(comparer);

        List = list;
    }

    protected OrderedSet(IComparer<TKey> comparer, IEqualityComparer<TKey> equalityComparer, SortedSet<KeyValuePair<TKey, TValue>> set)
    {
        _comparer = comparer;
        _equalityComparer = equalityComparer;
        KvComparer = new KeyValuePairComparer<TKey, TValue>(comparer);

        _set = set;
    }

    protected OrderedSet(IComparer<TKey> comparer, IEqualityComparer<TKey> equalityComparer, int capacity)
        : this(comparer, equalityComparer, new List<KeyValuePair<TKey, TValue>>(capacity))
    {
    }

    public OrderedSet(IComparer<TKey> comparer, IEqualityComparer<TKey> equalityComparer)
        : this(comparer, equalityComparer, 4)
    {
    }

    protected void TransformListToTree()
    {
        // Use the standard SortedSet(IEnumerable, IComparer) constructor rather than
        // ConstructFromSortedArray.  The latter requires the input to be SORTED; calling
        // it on an inverted list produces a structurally invalid red-black tree whose
        // in-order traversal is also inverted, making every subsequent CopyTo, Split,
        // and Remove produce corrupted data.  The standard constructor inserts each
        // element through the normal BST path (O(n log n)) and is correct for any
        // input order, including lists that may have accumulated inversions.
        _set = new SortedSet<KeyValuePair<TKey, TValue>>(List, KvComparer);
        List = null;
    }

    protected void TransformDictionaryToTree()
    {
        // Build the new tree from a snapshot of the current dictionary.
        var snapshot = _dictionary;
        if (snapshot == null) return; // another thread already completed the transform
        var newSet = new SortedSet<KeyValuePair<TKey, TValue>>(snapshot.Select(s => new KeyValuePair<TKey, TValue>(s.Key, s.Value)), KvComparer);
        // Write _set BEFORE _dictionary = null so that any thread whose volatile acquire-load
        // of _dictionary sees null is guaranteed to also see the fully-constructed newSet.
        _set = newSet;        // volatile release-store: publishes all tree-node writes
        _dictionary = null;   // volatile release-store: sentinel that the transform is done
    }

    protected void TransformListToDictionary()
    {
        _dictionary = new Dictionary<TKey, TValue>(List.Capacity, EqualityComparer);

        // Use indexer (not Add) so that duplicate keys in a corrupted list don't throw;
        // the last-wins behaviour is the correct upsert semantic here.
        foreach (var kv in List)
            _dictionary[kv.Key] = kv.Value;

        List = null;
    }

    /// <summary>
    /// clear all data and set ordered set to default list mode
    /// </summary>
    protected void Reset()
    {
        List = new List<KeyValuePair<TKey, TValue>>();
        _dictionary = null;
        _set = null;
    }

    private bool FindIndexes(KeyValuePair<TKey, TValue> from, bool hasFrom, KeyValuePair<TKey, TValue> to, bool hasTo, out int idxFrom, out int idxTo)
    {
        idxFrom = 0;
        idxTo = List.Count - 1;
        Debug.Assert(List.Count > 0);

        if (hasFrom)
        {
            var cmp = Comparer.Compare(from.Key, List[List.Count - 1].Key);
            if (cmp > 0)
                return false;

            if (cmp == 0)
            {
                idxFrom = idxTo;
                return true;
            }
        }

        if (hasTo)
        {
            var cmp = Comparer.Compare(to.Key, List[0].Key);
            if (cmp < 0)
                return false;

            if (cmp == 0)
            {
                idxTo = idxFrom;
                return true;
            }
        }

        if (hasFrom && Comparer.Compare(from.Key, List[0].Key) > 0)
        {
            idxFrom = List.BinarySearch(1, List.Count - 1, from, KvComparer);
            if (idxFrom < 0)
                idxFrom = ~idxFrom;
        }

        if (hasTo && Comparer.Compare(to.Key, List[List.Count - 1].Key) < 0)
        {
            idxTo = List.BinarySearch(idxFrom, List.Count - idxFrom, to, KvComparer);
            if (idxTo < 0)
                idxTo = ~idxTo - 1;
        }

        Debug.Assert(0 <= idxFrom);
        Debug.Assert(idxTo <= List.Count - 1);

        // The range [from, to] may fall entirely between two adjacent keys
        // (e.g. leaf has [10, 20], query is [12, 15]).  Binary search places
        // idxFrom past idxTo — this is an empty result, not a bug.
        if (idxFrom > idxTo)
            return false;

        return true;
    }

    public IOrderedSet<TKey, TValue> Split(int count)
    {
        if (List != null)
        {
            var right = List.Split(count);

            return new OrderedSet<TKey, TValue>(Comparer, EqualityComparer, right);
        }
        else
        {
            if (_dictionary != null)
                TransformDictionaryToTree();

            var right = _set.Split(count);

            return new OrderedSet<TKey, TValue>(Comparer, EqualityComparer, right);
        }
    }

    /// <summary>
    /// All keys in the input set must be less than all keys in the current set OR all keys in the input set must be greater than all keys in the current set.
    /// </summary>
    public void Merge(IOrderedSet<TKey, TValue> set)
    {
        if (set.Count == 0)
            return;

        if (Count == 0)
        {
            foreach (var x in set) //set.Forward()
            {
                // Defensive: guard against a source list that violates the sorted invariant.
                if (List.Count > 0 && _comparer.Compare(x.Key, List[List.Count - 1].Key) <= 0)
                    Add(x.Key, x.Value);
                else
                    List.Add(x);
            }

            return;
        }

        //Debug.Assert(comparer.Compare(this.Last.Key, set.First.Key) < 0 || comparer.Compare(this.First.Key, set.Last.Key) > 0);

        if (List != null)
        {
            if (KvComparer.Compare(set.Last, List[0]) < 0)
            {
                // Intended prepend: source reported last < our first.
                // Can't use InsertRange(0, ...) safely: if the source list has an inversion,
                // set.Last (=List[Count-1]) may not be the true maximum, making the check
                // unreliable.  Use per-element Add() which falls back to dict mode on any
                // out-of-order key, preventing inversion propagation.
                foreach (var kv in set.InternalEnumerate())
                    Add(kv.Key, kv.Value);
            }
            else if (KvComparer.Compare(set.First, List[List.Count - 1]) > 0)
            {
                // Intended append: source reported first > our last.
                // Iterate via Forward() (sorted for dict/tree sources; list-order for list
                // sources).  Add() appends directly when the invariant holds and falls back
                // to dict conversion when a source inversion is detected.
                foreach (var kv in set)
                    Add(kv.Key, kv.Value);
            }
            else
            {
                // Key ranges overlap — precondition violated; fall back to safe per-element Add.
                foreach (var kv in set.InternalEnumerate())
                    Add(kv.Key, kv.Value);
            }
        }
        else if (_dictionary != null)
        {
            // Use indexer so overlapping keys from the source overwrite rather than throw.
            foreach (var kv in set.InternalEnumerate())
                _dictionary[kv.Key] = kv.Value;
        }
        else //if (set != null)
        {
            foreach (var kv in set.InternalEnumerate())
                _set.Add(kv);
        }
    }

    #region IOrderedSet<TKey,TValue> Members

    public IComparer<TKey> Comparer => _comparer;

    public IEqualityComparer<TKey> EqualityComparer => _equalityComparer;

    public void Add(TKey key, TValue value)
    {
        var kv = new KeyValuePair<TKey, TValue>(key, value);

        if (_set != null)
        {
            _set.Replace(kv);
            return;
        }

        if (_dictionary != null)
        {
            _dictionary[kv.Key] = kv.Value;
            return;
        }

        if (List.Count == 0)
            List.Add(kv);
        else
        {
            var last = List[List.Count - 1];
            var cmp = _comparer.Compare(last.Key, kv.Key);

            if (cmp < 0)
                List.Add(kv);
            else if (cmp > 0)
            {
                TransformListToDictionary();
                _dictionary[kv.Key] = kv.Value;
            }
            else
                List[List.Count - 1] = kv;
        }
    }

    public void Add(KeyValuePair<TKey, TValue> item)
    {
        Add(item.Key, item.Value);
    }

    public void UnsafeAdd(TKey key, TValue value)
    {
        var kv = new KeyValuePair<TKey, TValue>(key, value);
        if (_set != null)
        {
            _set.Replace(kv);
            return;
        }

        if (_dictionary != null)
        {
            _dictionary[kv.Key] = kv.Value;
            return;
        }

        // List mode: caller guarantees monotone-ascending append.
        // Defensive check: if the invariant is violated fall back to safe Add().
        if (List.Count > 0 && _comparer.Compare(key, List[List.Count - 1].Key) <= 0)
        {
            Add(key, value);
            return;
        }

        List.Add(kv);
    }

    public bool Remove(TKey key)
    {
        var template = new KeyValuePair<TKey, TValue>(key, default(TValue));

        if (List != null)
            TransformListToDictionary();

        if (_dictionary != null)
        {
            var res = _dictionary.Remove(key);
            if (_dictionary.Count == 0)
                Reset();

            return res;
        }
        else
        {
            var res = _set.Remove(template);
            if (_set.Count == 0)
                Reset();

            return res;
        }
    }

    public bool Remove(TKey from, bool hasFrom, TKey to, bool hasTo)
    {
        if (Count == 0)
            return false;

        if (!hasFrom && !hasTo)
        {
            Clear();
            return true;
        }

        if (List != null)
            TransformListToTree();
        else if (_dictionary != null)
            TransformDictionaryToTree();

        var fromKey = hasFrom ? new KeyValuePair<TKey, TValue>(from, default(TValue)) : _set.Min;
        var toKey = hasTo ? new KeyValuePair<TKey, TValue>(to, default(TValue)) : _set.Max;

        var res = _set.Remove(fromKey, toKey);
        if (_set.Count == 0)
            Reset();

        return res;
    }

    public bool ContainsKey(TKey key)
    {
        return TryGetValue(key, out _);
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        var template = new KeyValuePair<TKey, TValue>(key, default(TValue));

        if (List != null)
        {
            var idx = List.BinarySearch(template, KvComparer);
            if (idx >= 0)
            {
                value = List[idx].Value;
                return true;
            }
        }
        else if (_dictionary != null)
            return _dictionary.TryGetValue(template.Key, out value);
        else
        {
            if (_set.TryGetValue(template, out var kv))
            {
                value = kv.Value;
                return true;
            }
        }

        value = default(TValue);
        return false;
    }

    public TValue this[TKey key]
    {
        get
        {
            if (!TryGetValue(key, out var value))
                throw new KeyNotFoundException("The key was not found.");

            return value;
        }
        set => Add(key, value);
    }

    public void Clear()
    {
        Reset();
    }

    public bool IsInternallyOrdered => _dictionary == null;

    public IEnumerable<KeyValuePair<TKey, TValue>> InternalEnumerate()
    {
        if (List != null)
            return List;
        if (_dictionary != null)
            return _dictionary.Select(s => new KeyValuePair<TKey, TValue>(s.Key, s.Value));
        return _set;
    }

    public void LoadFrom(KeyValuePair<TKey, TValue>[] array, int count, bool isOrdered)
    {
        if (isOrdered)
        {
            List = array.CreateList(count);
            _dictionary = null;
            _set = null;
        }
        else
        {
            List = null;
            _dictionary = new Dictionary<TKey, TValue>(count, EqualityComparer);
            _set = null;

            for (var i = 0; i < count; i++)
                _dictionary.Add(array[i].Key, array[i].Value);
        }
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo)
    {
        if (hasFrom && hasTo && _comparer.Compare(from, to) > 0)
            throw new ArgumentException("from > to");

        if (Count == 0)
            yield break;

        var fromKey = new KeyValuePair<TKey, TValue>(from, default(TValue));
        var toKey = new KeyValuePair<TKey, TValue>(to, default(TValue));

        if (List != null)
        {
            if (!FindIndexes(fromKey, hasFrom, toKey, hasTo, out var idxFrom, out var idxTo))
                yield break;

            for (var i = idxFrom; i <= idxTo; i++)
                yield return List[i];
        }
        else
        {
            if (_dictionary != null)
                TransformDictionaryToTree();

            var enumerable = hasFrom || hasTo ? _set.GetViewBetween(fromKey, toKey, hasFrom, hasTo) : _set;

            foreach (var x in enumerable)
                yield return x;
        }
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom)
    {
        if (hasFrom && hasTo && _comparer.Compare(from, to) > 0)
            throw new ArgumentException("from > to");

        if (Count == 0)
            yield break;

        var fromKey = new KeyValuePair<TKey, TValue>(from, default(TValue));
        var toKey = new KeyValuePair<TKey, TValue>(to, default(TValue));

        if (List != null)
        {
            if (!FindIndexes(fromKey, hasFrom, toKey, hasTo, out var idxFrom, out var idxTo))
                yield break;

            for (var i = idxTo; i >= idxFrom; i--)
                yield return List[i];
        }
        else
        {
            if (_dictionary != null)
                TransformDictionaryToTree();

            var enumerable = hasFrom || hasTo ? _set.GetViewBetween(fromKey, toKey, hasFrom, hasTo) : _set;

            foreach (var x in enumerable.Reverse())
                yield return x;
        }
    }

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        return Forward(default(TKey), false, default(TKey), false).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public KeyValuePair<TKey, TValue> First
    {
        get
        {
            if (Count == 0)
                throw new InvalidOperationException("The set is empty.");

            if (List != null)
                return List[0];

            if (_dictionary != null)
                TransformDictionaryToTree();

            return _set.Min;
        }
    }

    public KeyValuePair<TKey, TValue> Last
    {
        get
        {
            if (Count == 0)
                throw new InvalidOperationException("The set is empty.");

            if (List != null)
                return List[List.Count - 1];

            if (_dictionary != null)
                TransformDictionaryToTree();

            return _set.Max;
        }
    }

    public int Count
    {
        get
        {
            if (List != null)
                return List.Count;

            if (_dictionary != null)
                return _dictionary.Count;

            return _set.Count;
        }
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> ForwardExclusive(
        TKey from, bool hasFrom, bool fromExclusive,
        TKey to,   bool hasTo,   bool toExclusive)
    {
        if (hasFrom && hasTo && _comparer.Compare(from, to) > 0)
            yield break;
        if (hasFrom && hasTo && _comparer.Compare(from, to) == 0 && (fromExclusive || toExclusive))
            yield break;

        if (Count == 0)
            yield break;

        var fromKey = new KeyValuePair<TKey, TValue>(from, default(TValue));
        var toKey   = new KeyValuePair<TKey, TValue>(to,   default(TValue));

        if (List != null)
        {
            if (!FindIndexes(fromKey, hasFrom, toKey, hasTo, out var idxFrom, out var idxTo))
                yield break;

            // Adjust indices for exclusive endpoints: O(1) arithmetic, no per-record call
            if (fromExclusive && hasFrom && _comparer.Compare(List[idxFrom].Key, from) == 0)
                idxFrom++;
            if (toExclusive   && hasTo   && idxTo >= idxFrom && _comparer.Compare(List[idxTo].Key, to) == 0)
                idxTo--;

            for (var i = idxFrom; i <= idxTo; i++)
                yield return List[i];
        }
        else
        {
            if (_dictionary != null)
                TransformDictionaryToTree();

            var enumerable = hasFrom || hasTo ? _set.GetViewBetween(fromKey, toKey, hasFrom, hasTo) : _set;

            // At most one skip at the start (fromExclusive) and one break at the end (toExclusive)
            bool skipFrom = fromExclusive && hasFrom;
            foreach (var x in enumerable)
            {
                if (skipFrom && _comparer.Compare(x.Key, from) == 0) { skipFrom = false; continue; }
                skipFrom = false;
                if (toExclusive && hasTo && _comparer.Compare(x.Key, to) == 0) yield break;
                yield return x;
            }
        }
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> BackwardExclusive(
        TKey to,   bool hasTo,   bool toExclusive,
        TKey from, bool hasFrom, bool fromExclusive)
    {
        if (hasFrom && hasTo && _comparer.Compare(from, to) > 0)
            yield break;
        if (hasFrom && hasTo && _comparer.Compare(from, to) == 0 && (fromExclusive || toExclusive))
            yield break;

        if (Count == 0)
            yield break;

        var fromKey = new KeyValuePair<TKey, TValue>(from, default(TValue));
        var toKey   = new KeyValuePair<TKey, TValue>(to,   default(TValue));

        if (List != null)
        {
            if (!FindIndexes(fromKey, hasFrom, toKey, hasTo, out var idxFrom, out var idxTo))
                yield break;

            // Backward start = idxTo (highest), end = idxFrom (lowest)
            if (toExclusive   && hasTo   && _comparer.Compare(List[idxTo].Key, to)     == 0)
                idxTo--;
            if (fromExclusive && hasFrom && idxFrom <= idxTo && _comparer.Compare(List[idxFrom].Key, from) == 0)
                idxFrom++;

            for (var i = idxTo; i >= idxFrom; i--)
                yield return List[i];
        }
        else
        {
            if (_dictionary != null)
                TransformDictionaryToTree();

            var enumerable = hasFrom || hasTo ? _set.GetViewBetween(fromKey, toKey, hasFrom, hasTo) : _set;

            bool skipTo = toExclusive && hasTo;
            foreach (var x in enumerable.Reverse())
            {
                if (skipTo && _comparer.Compare(x.Key, to) == 0) { skipTo = false; continue; }
                skipTo = false;
                if (fromExclusive && hasFrom && _comparer.Compare(x.Key, from) == 0) yield break;
                yield return x;
            }
        }
    }

    // ─── Direct-access API for zero-yield leaf iteration ──────────────────

    public List<KeyValuePair<TKey, TValue>>? InternalList => List;

    public bool TryGetSortedRange(
        TKey from, bool hasFrom, bool fromExclusive,
        TKey to,   bool hasTo,   bool toExclusive,
        out int startIndex, out int endIndex)
    {
        startIndex = endIndex = 0;

        if (List == null || List.Count == 0)
            return false;

        if (hasFrom && hasTo && _comparer.Compare(from, to) > 0)
            return false;
        if (hasFrom && hasTo && _comparer.Compare(from, to) == 0 && (fromExclusive || toExclusive))
            return false;

        var fromKey = new KeyValuePair<TKey, TValue>(from, default(TValue));
        var toKey   = new KeyValuePair<TKey, TValue>(to,   default(TValue));

        if (!FindIndexes(fromKey, hasFrom, toKey, hasTo, out startIndex, out endIndex))
            return false;

        // Adjust for exclusive bounds: O(1) index arithmetic
        if (fromExclusive && hasFrom && startIndex <= endIndex &&
            _comparer.Compare(List[startIndex].Key, from) == 0)
            startIndex++;

        if (toExclusive && hasTo && endIndex >= startIndex &&
            _comparer.Compare(List[endIndex].Key, to) == 0)
            endIndex--;

        return startIndex <= endIndex;
    }

    /// <summary>
    /// Explicit implementation of <see cref="IOrderedSet{TKey,TValue}.CountRange"/>
    /// that bypasses the DIM dispatch chain, calling <see cref="TryGetSortedRange"/> directly.
    /// </summary>
    public int CountRange(
        TKey from, bool hasFrom, bool fromExclusive,
        TKey to,   bool hasTo,   bool toExclusive)
    {
        if (TryGetSortedRange(from, hasFrom, fromExclusive, to, hasTo, toExclusive,
                              out var si, out var ei))
            return ei - si + 1;
        return -1;
    }

    #endregion
}
