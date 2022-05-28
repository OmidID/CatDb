﻿using CatDb.Data;
using CatDb.General.Collections;
using CatDb.General.Compression;
using CatDb.General.Persist;

namespace CatDb.Database
{
    public class OrderedSetPersist : IPersist<IOrderedSet<IData, IData>>
    {
        public const byte VERSION = 40;

        private IIndexerPersist<IData> keyIndexerPersist;
        private IIndexerPersist<IData> recordIndexerPersist;

        private IPersist<IData> keyPersist;
        private IPersist<IData> recordPersist;

        private IOrderedSetFactory orderedSetFactory;

        private bool verticalCompression;

        public OrderedSetPersist(IIndexerPersist<IData> keyIndexerPersist, IIndexerPersist<IData> recordIndexerPersist, IOrderedSetFactory orderedSetFactory)
        {
            this.keyIndexerPersist = keyIndexerPersist;
            this.recordIndexerPersist = recordIndexerPersist;
            this.orderedSetFactory = orderedSetFactory;
            verticalCompression = true;
        }

        public OrderedSetPersist(IPersist<IData> keyPersist, IPersist<IData> recordPersist, IOrderedSetFactory orderedSetFactory)
        {
            this.keyPersist = keyPersist;
            this.recordPersist = recordPersist;
            this.orderedSetFactory = orderedSetFactory;
            verticalCompression = false;
        }

        private void WriteRaw(BinaryWriter writer, IOrderedSet<IData, IData> data)
        {
            lock (data)
            {
                writer.Write(data.Count);
                writer.Write(data.IsInternallyOrdered);

                foreach (var kv in data.InternalEnumerate())
                {
                    keyPersist.Write(writer, kv.Key);
                    recordPersist.Write(writer, kv.Value);
                }
            }
        }

        private IOrderedSet<IData, IData> ReadRaw(BinaryReader reader)
        {
            var count = reader.ReadInt32();
            var isOrdered = reader.ReadBoolean();

            var data = orderedSetFactory.Create();

            var array = new KeyValuePair<IData, IData>[count];

            for (var i = 0; i < count; i++)
            {
                var key = keyPersist.Read(reader);
                var record = recordPersist.Read(reader);
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
                keyIndexerPersist.Store(new BinaryWriter(streams[0]), (idx) => { return rows[idx].Key; }, rows.Length);
            };

            actions[1] = () =>
            {
                streams[1] = new MemoryStream();
                recordIndexerPersist.Store(new BinaryWriter(streams[1]), (idx) => { return rows[idx].Value; }, rows.Length);
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
                using (var ms = new MemoryStream(buffers[0]))
                    keyIndexerPersist.Load(new BinaryReader(ms), (idx, value) => { array[idx].SetKey(value); }, count);
            };

            actions[1] = () =>
            {
                using (var ms = new MemoryStream(buffers[1]))
                    recordIndexerPersist.Load(new BinaryReader(ms), (idx, value) => { array[idx].SetValue(value); }, count);
            };

            var task = Task.Factory.StartNew(actions[1]);
            actions[0]();
            task.Wait();

            var data = orderedSetFactory.Create();
            data.LoadFrom(array, count, isOrdered);

            return data;
        }

        public void Write(BinaryWriter writer, IOrderedSet<IData, IData> item)
        {
            writer.Write(VERSION);

            if (verticalCompression)
                WriteVertical(writer, item);
            else
                WriteRaw(writer, item);
        }

        public IOrderedSet<IData, IData> Read(BinaryReader reader)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid DataContainerPersist version.");

            if (verticalCompression)
                return ReadVertical(reader);
            else
                return ReadRaw(reader);
        }
    }
}
