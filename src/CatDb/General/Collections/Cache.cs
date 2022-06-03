using System.Collections;

namespace CatDb.General.Collections
{
    public class Cache<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
        //where TKey : IEquatable<TKey>//, IComparable<TKey>
    {
        private readonly IDictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>> _mapping;//mapping between link and element in Items
        private readonly LinkedList<KeyValuePair<TKey, TValue>> _items = new();//The newer and/or most used elements emerges on top(begining)
        private int _capacity;

        public readonly object SyncRoot = new();
        public event OverflowDelegate Overflow;

        //Comparer<TKey>.Default
        public Cache(int capacity, IComparer<TKey> comparer)
        {
            _capacity = capacity;
            _mapping = new SortedDictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>>(comparer);
        }

        //EqualityComparer<TKey>.Default
        public Cache(int capacity, IEqualityComparer<TKey> comparer)
        {
            _capacity = capacity;
            _mapping = new Dictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>>(comparer);
        }

        public Cache(int capacity)
            : this(capacity, EqualityComparer<TKey>.Default)
        {
        }

        public TValue this[TKey key]
        {
            get => Retrieve(key);
            set => Packet(key, value);
        }

        public TValue Packet(TKey key, TValue value)
        {
            TValue result;
            if (_mapping.TryGetValue(key, out var node))
            {
                result = node.Value.Value;
                node.Value = new KeyValuePair<TKey, TValue>(key, value);
                Refresh(node);
            }
            else
            {
                result = default(TValue);
                _mapping[key] = _items.AddFirst(new KeyValuePair<TKey, TValue>(key, value));
                ClearOverflowItems();
            }

            return result;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            if (!_mapping.TryGetValue(key, out var node))
            {
                value = default(TValue);
                return false;
            }

            Refresh(node);
            value = node.Value.Value;
            return true;
        }

        public TValue Retrieve(TKey key)
        {
            TryGetValue(key, out var value);

            return value;
        }

        public TValue Exclude(TKey key)
        {
            if (!_mapping.TryGetValue(key, out var node))
                return default(TValue);

            _mapping.Remove(key);
            _items.Remove(node);

            return node.Value.Value;
        }

        public void Clear()
        {
            _mapping.Clear();
            _items.Clear();
        }

        private void Refresh(LinkedListNode<KeyValuePair<TKey, TValue>> node)
        {
            if (node != _items.First)
            {
                _items.Remove(node);
                _items.AddFirst(node);
            }
        }

        public int Capacity
        {
            get => _capacity;
            set
            {
                if (_capacity == value)
                    return;

                _capacity = value;
                ClearOverflowItems();
            }
        }

        public int Count => _items.Count;

        public bool IsOverflow => (_items.Count > Capacity);

        public KeyValuePair<TKey, TValue> ExcludeLastItem()
        {
            var item = _items.Last.Value;
            _mapping.Remove(_items.Last.Value.Key);
            _items.RemoveLast();
            return item;
        }

        private void ClearOverflowItems()
        {
            while (IsOverflow)
            {
                var item = ExcludeLastItem();
                if (Overflow != null)
                    Overflow(item);
            }
        }

        #region IEnumerable<KeyValuePair<TKey,TValue>> Members

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            var node = _items.Last;
            while (node != null)
            {
                yield return node.Value;

                node = node.Previous;
            }
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        public delegate void OverflowDelegate(KeyValuePair<TKey, TValue> item);
    }
}
