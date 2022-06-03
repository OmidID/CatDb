using CatDb.General.Extensions;

namespace CatDb.General.Persist
{
    public class StringIndexerPersist : IIndexerPersist<String>
    {
        private const byte VERSION = 40;

        private const int NULL_ID = -1;
        private const double PERCENT = 38.2;

        public void Store(BinaryWriter writer, Func<int, string> values, int count)
        {
            writer.Write(VERSION);
            
            var mapCapacity = (int)((PERCENT / 100) * count);
            var map = new Dictionary<string, int>(/*MAP_CAPACITY*/); //optimistic variant

            var id = 0;
            var indexes = new int[count];
            var mode = PersistMode.Dictionary;

            for (var i = 0; i < count; i++)
            {
                var value = values(i);
                if (value == null)
                {
                    indexes[i] = NULL_ID;
                    continue;
                }

                if (map.TryGetValue(value, out var mapId))
                {
                    indexes[i] = mapId;
                    continue;
                }

                if (map.Count == mapCapacity)
                {
                    mode = PersistMode.Raw;
                    break;
                }

                map.Add(value, id);
                indexes[i] = id;
                id++;
            }

            writer.Write((byte)mode);

            switch (mode)
            {
                case PersistMode.Raw:
                    {
                        new Raw().Store(writer, values, count);
                    }
                    break;

                case PersistMode.Dictionary:
                    {
                        writer.Write(map.Count);
                        foreach (var kv in map.OrderBy(x => x.Value))
                            writer.Write(kv.Key);

                        new Int32IndexerPersist().Store(writer, idx => { return indexes[idx]; }, count);
                    }
                    break;

                default:
                    throw new NotSupportedException(mode.ToString());
            }
        }

        public void Load(BinaryReader reader, Action<int, string> values, int count)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid StringIndexerPersist version.");

            var mode = (PersistMode)reader.ReadByte();

            switch (mode)
            {
                case PersistMode.Raw:
                    {
                        new Raw().Load(reader, values, count);
                    }
                    break;

                case PersistMode.Dictionary:
                    {
                        var map = new string[reader.ReadInt32()];
                        for (var i = 0; i < map.Length; i++)
                            map[i] = reader.ReadString();

                        new Int32IndexerPersist().Load(reader, (idx, value) => { values(idx, value == NULL_ID ? null : map[value]); }, count);
                    }
                    break;

                default:
                    throw new NotSupportedException(mode.ToString());
            }
        }

        public class Raw : IIndexerPersist<String>
        {
            public void Store(BinaryWriter writer, Func<int, string> values, int count)
            {
                var buffer = new byte[(int)Math.Ceiling(count / 8.0)];

                var array = new string[count];
                var length = 0;

                for (var i = 0; i < count; i++)
                {
                    var value = values(i);
                    if (value != null)
                    {
                        buffer.SetBit(i, 1);
                        array[length++] = value;
                    }
                    //else
                    //    buffer.SetBit(i, 0);
                }

                writer.Write(buffer);

                for (var i = 0; i < length; i++)
                    writer.Write(array[i]);
            }

            public void Load(BinaryReader reader, Action<int, string> values, int count)
            {
                var buffer = reader.ReadBytes((int)Math.Ceiling(count / 8.0));

                for (var i = 0; i < count; i++)
                    values(i, buffer.GetBit(i) == 1 ? reader.ReadString() : null);
            }
        }

        private enum PersistMode : byte
        {
            Raw,
            Dictionary
        }
    }
}