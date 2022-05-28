namespace CatDb
{
    public struct KeyValuePair<TKey, TValue>
    {
        private TKey _key;
        private TValue _value;

        public KeyValuePair(TKey key, TValue value)
        {
            _key = key;
            _value = value;
        }

        public TKey Key => _key;

        public TValue Value => _value;

        internal void SetKey(TKey key) => _key = key;
        internal void SetValue(TValue value) => _value = value;
        internal void SetKeyValue(TKey key, TValue value)
        {
            _key = key;
            _value = value;
        }

        public override string ToString()
        {
            return $"{_key}:{_value}";
        }
    }
}
