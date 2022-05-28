using CatDb.Data;
using CatDb.General.Collections;
using CatDb.General.Compression;
using CatDb.General.Persist;

namespace CatDb.Database
{
    public class OrderedSetPersist : IPersist<IOrderedSet<IData, IData>>
    {
        private const byte VERSION = 40;

        private readonly IIndexerPersist<IData> _keyIndexerPersist;
        private readonly IIndexerPersist<IData> _recordIndexerPersist;

        private readonly IPersist<IData> _keyPersist;
        private readonly IPersist<IData> _recordPersist;

        private readonly IOrderedSetFactory _orderedSetFactory;

        private readonly bool _verticalCompression;

        public OrderedSetPersist(IIndexerPersist<IData> keyIndexerPersist, IIndexerPersist<IData> recordIndexerPersist, IOrderedSetFactory orderedSetFactory)
        {
            this._keyIndexerPersist = keyIndexerPersist;
            this._recordIndexerPersist = recordIndexerPersist;
            this._orderedSetFactory = orderedSetFactory;
            _verticalCompression = true;
        }

        public OrderedSetPersist(IPersist<IData> keyPersist, IPersist<IData> recordPersist, IOrderedSetFactory orderedSetFactory)
        {
            this._keyPersist = keyPersist;
            this._recordPersist = recordPersist;
            this._orderedSetFactory = orderedSetFactory;
            _verticalCompression = false;
        }

        private void WriteRaw(BinaryWriter writer, IOrderedSet<IData, IData> data)
        {
            lock (data)
            {
                writer.Write(data.Count);
                writer.Write(data.IsInternallyOrdered);

                foreach (var kv in data.InternalEnumerate())
                {
                    _keyPersist.Write(writer, kv.Key);
                    _recordPersist.Write(writer, kv.Value);
                }
            }
        }

        private IOrderedSet<IData, IData> ReadRaw(BinaryReader reader)
        {
            var count = reader.ReadInt32();
            var isOrdered = reader.ReadBoolean();

            var data = _orderedSetFactory.Create();

            var array = new KeyValuePair<IData, IData>[count];

            for (var i = 0; i < count; i++)
            {
                var key = _keyPersist.Read(reader);
                var record = _recordPersist.Read(reader);
                array[i] = new KeyValuePair<IData, IData>(key, record);
            }

            data.LoadFrom(array, count, isOrdered);

            return data;
        }

        private void WriteVertical(BinaryWriter writer, IOrderedSet<IData, IData> data)
        {
            KeyValuePair<IData, IData>[] rows;

            bool isInternallyOrdered;

            lock (data)
            {
                isInternallyOrdered = data.IsInternallyOrdered;

                rows = new KeyValuePair<IData, IData>[data.Count];
                var index = 0;
                foreach (var kv in data.InternalEnumerate())
                    rows[index++] = kv;

                CountCompression.Serialize(writer, checked((ulong)rows.Length));
                writer.Write(data.IsInternallyOrdered);
            }

            var actions = new Action[2];
            var streams = new MemoryStream[2];

            actions[0] = () =>
            {
                streams[0] = new MemoryStream();
                _keyIndexerPersist.Store(new BinaryWriter(streams[0]), (idx) => { return rows[idx].Key; }, rows.Length);
            };

            actions[1] = () =>
            {
                streams[1] = new MemoryStream();
                _recordIndexerPersist.Store(new BinaryWriter(streams[1]), (idx) => { return rows[idx].Value; }, rows.Length);
            };

            Parallel.Invoke(actions);

            foreach (var stream in streams)
            {
                using (stream)
                {
                    CountCompression.Serialize(writer, checked((ulong)stream.Length));
                    writer.Write(stream.GetBuffer(), 0, (int)stream.Length);
                }
            }
        }

        //private static readonly KeyValuePairHelper<IData, IData> helper = new KeyValuePairHelper<IData, IData>();

        private IOrderedSet<IData, IData> ReadVertical(BinaryReader reader)
        {
            var count = (int)CountCompression.Deserialize(reader);
            var isOrdered = reader.ReadBoolean();

            var array = new KeyValuePair<IData, IData>[count];

            var actions = new Action[2];
            var buffers = new byte[2][];

            for (var i = 0; i < buffers.Length; i++)
                buffers[i] = reader.ReadBytes((int)CountCompression.Deserialize(reader));

            actions[0] = () =>
            {
                using var ms = new MemoryStream(buffers[0]);
                _keyIndexerPersist.Load(new BinaryReader(ms), (idx, value) => { array[idx].SetKey(value); }, count);
            };

            actions[1] = () =>
            {
                using var ms = new MemoryStream(buffers[1]);
                _recordIndexerPersist.Load(new BinaryReader(ms), (idx, value) => { array[idx].SetValue(value); }, count);
            };

            var task = Task.Factory.StartNew(actions[1]);
            actions[0]();
            task.Wait();

            var data = _orderedSetFactory.Create();
            data.LoadFrom(array, count, isOrdered);

            return data;
        }

        public void Write(BinaryWriter writer, IOrderedSet<IData, IData> item)
        {
            writer.Write(VERSION);

            if (_verticalCompression)
                WriteVertical(writer, item);
            else
                WriteRaw(writer, item);
        }

        public IOrderedSet<IData, IData> Read(BinaryReader reader)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid DataContainerPersist version.");

            if (_verticalCompression)
                return ReadVertical(reader);
            else
                return ReadRaw(reader);
        }
    }
}
