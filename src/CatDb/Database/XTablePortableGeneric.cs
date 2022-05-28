using System.Collections;
using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public class XTablePortable<TKey, TRecord> : ITable<TKey, TRecord>
    {
        public ITable<IData, IData> Table { get; private set; }
        public ITransformer<TKey, IData> KeyTransformer { get; private set; }
        public ITransformer<TRecord, IData> RecordTransformer { get; private set; }

        public XTablePortable(ITable<IData, IData> table, ITransformer<TKey, IData> keyTransformer = null, ITransformer<TRecord, IData> recordTransformer = null)
        {
            if (table == null)
                throw new ArgumentNullException("table");

            Table = table;

            if (keyTransformer == null)
                keyTransformer = new DataTransformer<TKey>(table.Descriptor.KeyType);

            if (recordTransformer == null)
                recordTransformer = new DataTransformer<TRecord>(table.Descriptor.RecordType);

            KeyTransformer = keyTransformer;
            RecordTransformer = recordTransformer;
        }

        #region ITable<TKey, TRecord> Membres

        public TRecord this[TKey key]
        {
            get
            {
                var ikey = KeyTransformer.To(key);
                var irec = Table[ikey];
                
                return RecordTransformer.From(irec);
            }
            set
            {
                var ikey = KeyTransformer.To(key);
                var irec = RecordTransformer.To(value);

                Table[ikey] = irec;
            }
        }

        public void Replace(TKey key, TRecord record)
        {
            var ikey = KeyTransformer.To(key);
            var irec = RecordTransformer.To(record);

            Table.Replace(ikey, irec);
        }

        public void InsertOrIgnore(TKey key, TRecord record)
        {
            var ikey = KeyTransformer.To(key);
            var irec = RecordTransformer.To(record);

            Table.InsertOrIgnore(ikey, irec);
        }

        public void Delete(TKey key)
        {
            var ikey = KeyTransformer.To(key);

            Table.Delete(ikey);
        }

        public void Delete(TKey fromKey, TKey toKey)
        {
            var ifrom = KeyTransformer.To(fromKey);
            var ito = KeyTransformer.To(toKey);

            Table.Delete(ifrom, ito);
        }

        public void Clear()
        {
            Table.Clear();
        }

        public bool Exists(TKey key)
        {
            var ikey = KeyTransformer.To(key);

            return Table.Exists(ikey);
        }

        public bool TryGet(TKey key, out TRecord record)
        {
            var ikey = KeyTransformer.To(key);

            IData irec;
            if (!Table.TryGet(ikey, out irec))
            {
                record = default(TRecord);
                return false;
            }

            record = RecordTransformer.From(irec);

            return true;
        }

        public TRecord Find(TKey key)
        {
            var ikey = KeyTransformer.To(key);

            var irec = Table.Find(ikey);
            if (irec == null)
                return default(TRecord);

            var record = RecordTransformer.From(irec);

            return record;
        }

        public TRecord TryGetOrDefault(TKey key, TRecord defaultRecord)
        {
            var ikey = KeyTransformer.To(key);
            var idefaultRec = RecordTransformer.To(defaultRecord);
            var irec = Table.TryGetOrDefault(ikey, idefaultRec);

            var record = RecordTransformer.From(irec);

            return record;
        }

        public KeyValuePair<TKey, TRecord>? FindNext(TKey key)
        {
            var ikey = KeyTransformer.To(key);

            var kv = Table.FindNext(ikey);
            if (!kv.HasValue)
                return null;

            var k = KeyTransformer.From(kv.Value.Key);
            var r = RecordTransformer.From(kv.Value.Value);

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindAfter(TKey key)
        {
            var ikey = KeyTransformer.To(key);

            var kv = Table.FindAfter(ikey);
            if (!kv.HasValue)
                return null;

            var k = KeyTransformer.From(kv.Value.Key);
            var r = RecordTransformer.From(kv.Value.Value);

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindPrev(TKey key)
        {
            var ikey = KeyTransformer.To(key);

            var kv = Table.FindPrev(ikey);
            if (!kv.HasValue)
                return null;

            var k = KeyTransformer.From(kv.Value.Key);
            var r = RecordTransformer.From(kv.Value.Value);

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindBefore(TKey key)
        {
            var ikey = KeyTransformer.To(key);

            var kv = Table.FindBefore(ikey);
            if (!kv.HasValue)
                return null;

            var k = KeyTransformer.From(kv.Value.Key);
            var r = RecordTransformer.From(kv.Value.Value);

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Forward()
        {
            foreach (var kv in Table.Forward())
            {
                var key = KeyTransformer.From(kv.Key);
                var rec = RecordTransformer.From(kv.Value);

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo)
        {
            var ifrom = hasFrom ? KeyTransformer.To(from) : null;
            var ito = hasTo ? KeyTransformer.To(to) : null;

            foreach (var kv in Table.Forward(ifrom, hasFrom, ito, hasTo))
            {
                var key = KeyTransformer.From(kv.Key);
                var rec = RecordTransformer.From(kv.Value);

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Backward()
        {
            foreach (var kv in Table.Backward())
            {
                var key = KeyTransformer.From(kv.Key);
                var rec = RecordTransformer.From(kv.Value);

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom)
        {
            var ito = hasTo ? KeyTransformer.To(to) : null;
            var ifrom = hasFrom ? KeyTransformer.To(from) : null;
            
            foreach (var kv in Table.Backward(ito, hasTo, ifrom, hasFrom))
            {
                var key = KeyTransformer.From(kv.Key);
                var rec = RecordTransformer.From(kv.Value);

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public KeyValuePair<TKey, TRecord> FirstRow
        {
            get
            {
                var kv = Table.FirstRow;

                var key = KeyTransformer.From(kv.Key);
                var rec = RecordTransformer.From(kv.Value);

                return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public KeyValuePair<TKey, TRecord> LastRow
        {
            get
            {
                var kv = Table.LastRow;

                var key = KeyTransformer.From(kv.Key);
                var rec = RecordTransformer.From(kv.Value);

                return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public long Count()
        {
            return Table.Count();
        }

        public IDescriptor Descriptor => Table.Descriptor;

        #endregion

        #region IEnumerable<KeyValuePair<TKey, TRecord>> Members

        public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator()
        {
            return Forward().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }
}
