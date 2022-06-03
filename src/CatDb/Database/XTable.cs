﻿using System.Collections;
using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public class XTable<TKey, TRecord> : ITable<TKey, TRecord>
    {
        public ITable<IData, IData> Table { get; private set; }

        public XTable(ITable<IData, IData> table)
        {
            if (table == null)
                throw new ArgumentNullException("table");
            
            Table = table;
        }

        #region ITable<TKey, TRecord> Membres

        public TRecord this[TKey key]
        {
            get
            {
                IData ikey = new Data<TKey>(key);
                var irec = Table[ikey];

                return ((Data<TRecord>)irec).Value;
            }
            set
            {
                IData ikey = new Data<TKey>(key);
                IData irec = new Data<TRecord>(value);

                Table[ikey] = irec;
            }
        }

        public void Replace(TKey key, TRecord record)
        {
            IData ikey = new Data<TKey>(key);
            IData irec = new Data<TRecord>(record);

            Table.Replace(ikey, irec);
        }

        public void InsertOrIgnore(TKey key, TRecord record)
        {
            IData ikey = new Data<TKey>(key);
            IData irec = new Data<TRecord>(record);

            Table.InsertOrIgnore(ikey, irec);
        }

        public void Delete(TKey key)
        {
            IData ikey = new Data<TKey>(key);

            Table.Delete(ikey);
        }

        public void Delete(TKey fromKey, TKey toKey)
        {
            IData ifrom = new Data<TKey>(fromKey);
            IData ito = new Data<TKey>(toKey);

            Table.Delete(ifrom, ito);
        }

        public void Clear()
        {
            Table.Clear();
        }

        public bool Exists(TKey key)
        {
            IData ikey = new Data<TKey>(key);

            return Table.Exists(ikey);
        }

        public bool TryGet(TKey key, out TRecord record)
        {
            IData ikey = new Data<TKey>(key);

            if (!Table.TryGet(ikey, out var irec))
            {
                record = default(TRecord);
                return false;
            }

            record = ((Data<TRecord>)irec).Value;

            return true;
        }

        public TRecord Find(TKey key)
        {
            IData ikey = new Data<TKey>(key);

            var irec = Table.Find(ikey);
            if (irec == null)
                return default(TRecord);

            var record = ((Data<TRecord>)irec).Value;

            return record;
        }

        public TRecord TryGetOrDefault(TKey key, TRecord defaultRecord)
        {
            IData ikey = new Data<TKey>(key);
            IData idefaultRec = new Data<TRecord>(defaultRecord);
            var irec = Table.TryGetOrDefault(ikey, idefaultRec);

            var record = ((Data<TRecord>)irec).Value;

            return record;
        }

        public KeyValuePair<TKey, TRecord>? FindNext(TKey key)
        {
            IData ikey = new Data<TKey>(key);

            var kv = Table.FindNext(ikey);
            if (!kv.HasValue)
                return null;

            var k = ((Data<TKey>)kv.Value.Key).Value;
            var r = ((Data<TRecord>)kv.Value.Value).Value;

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindAfter(TKey key)
        {
            IData ikey = new Data<TKey>(key);

            var kv = Table.FindAfter(ikey);
            if (!kv.HasValue)
                return null;

            var k = ((Data<TKey>)kv.Value.Key).Value;
            var r = ((Data<TRecord>)kv.Value.Value).Value;

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindPrev(TKey key)
        {
            IData ikey = new Data<TKey>(key);

            var kv = Table.FindPrev(ikey);
            if (!kv.HasValue)
                return null;

            var k = ((Data<TKey>)kv.Value.Key).Value;
            var r = ((Data<TRecord>)kv.Value.Value).Value;

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindBefore(TKey key)
        {
            IData ikey = new Data<TKey>(key);

            var kv = Table.FindBefore(ikey);
            if (!kv.HasValue)
                return null;

            var k = ((Data<TKey>)kv.Value.Key).Value;
            var r = ((Data<TRecord>)kv.Value.Value).Value;

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Forward()
        {
            foreach (var kv in Table.Forward())
            {
                var key = ((Data<TKey>)kv.Key).Value;
                var rec = ((Data<TRecord>)kv.Value).Value;

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo)
        {
            IData ifrom = hasFrom ? new Data<TKey>(from) : null;
            IData ito = hasTo ? new Data<TKey>(to) : null;

            foreach (var kv in Table.Forward(ifrom, hasFrom, ito, hasTo))
            {
                var key = ((Data<TKey>)kv.Key).Value;
                var rec = ((Data<TRecord>)kv.Value).Value;

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Backward()
        {
            foreach (var kv in Table.Backward())
            {
                var key = ((Data<TKey>)kv.Key).Value;
                var rec = ((Data<TRecord>)kv.Value).Value;

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom)
        {
            IData ito = hasTo ? new Data<TKey>(to) : null;
            IData ifrom = hasFrom ? new Data<TKey>(from) : null;

            foreach (var kv in Table.Backward(ito, hasTo, ifrom, hasFrom))
            {
                var key = ((Data<TKey>)kv.Key).Value;
                var rec = ((Data<TRecord>)kv.Value).Value;

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public KeyValuePair<TKey, TRecord> FirstRow
        {
            get
            {
                var kv = Table.FirstRow;

                var key = ((Data<TKey>)kv.Key).Value;
                var rec = ((Data<TRecord>)kv.Value).Value;

                return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public KeyValuePair<TKey, TRecord> LastRow
        {
            get
            {
                var kv = Table.LastRow;

                var key = ((Data<TKey>)kv.Key).Value;
                var rec = ((Data<TRecord>)kv.Value).Value;

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
