using System.Collections;
using System.Diagnostics;
using CatDb.General.Comparers;
using CatDb.General.Extensions;

namespace CatDb.General.Collections
{
    public class OrderedSet<TKey, TValue> : IOrderedSet<TKey, TValue>
    {
        protected List<KeyValuePair<TKey, TValue>> List;
        private Dictionary<TKey, TValue> _dictionary;
        private SortedSet<KeyValuePair<TKey, TValue>> _set;

        private readonly IComparer<TKey> _comparer;
        private readonly IEqualityComparer<TKey> _equalityComparer;
        protected KeyValuePairComparer<TKey, TValue> KvComparer;

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
            _set = new SortedSet<KeyValuePair<TKey, TValue>>(KvComparer);
            _set.ConstructFromSortedArray(List.GetArray(), 0, List.Count);
            List = null;
        }

        protected void TransformDictionaryToTree()
        {
            _set = new SortedSet<KeyValuePair<TKey, TValue>>(_dictionary.Select(s => new KeyValuePair<TKey, TValue>(s.Key, s.Value)), KvComparer);
            _dictionary = null;
        }

        protected void TransformListToDictionary()
        {
            _dictionary = new Dictionary<TKey, TValue>(List.Capacity, EqualityComparer);

            foreach (var kv in List)
                _dictionary.Add(kv.Key, kv.Value);

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
            Debug.Assert(idxFrom <= idxTo);
            Debug.Assert(idxTo <= List.Count - 1);

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
                    List.Add(x);

                return;
            }

            //Debug.Assert(comparer.Compare(this.Last.Key, set.First.Key) < 0 || comparer.Compare(this.First.Key, set.Last.Key) > 0);

            if (List != null)
            {
                var idx = KvComparer.Compare(set.Last, List[0]) < 0 ? 0 : List.Count;
                List.InsertRange(idx, set);
            }
            else if (_dictionary != null)
            {
                foreach (var kv in set.InternalEnumerate())
                    _dictionary.Add(kv.Key, kv.Value); //there should be no exceptions
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

        #endregion
    }
}
