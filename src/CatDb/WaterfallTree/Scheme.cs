using CatDb.Data;
using System.Collections;
using System.Collections.Concurrent;

namespace CatDb.WaterfallTree
{
    public class Scheme : IEnumerable<KeyValuePair<long, Locator>>
    {
        private const byte VERSION = 40;

        private long _locatorId = Locator.Min.Id;

        private readonly ConcurrentDictionary<long, Locator> _map = new();

        private long ObtainPathId()
        {
            return Interlocked.Increment(ref _locatorId);
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(VERSION);

            writer.Write(_locatorId);
            writer.Write(_map.Count);

            foreach (var kv in _map)
            {
                var locator = kv.Value;
                locator.Serialize(writer);
            }
        }

        public static Scheme Deserialize(BinaryReader reader)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid Scheme version.");

            var scheme = new Scheme
            {
                _locatorId = reader.ReadInt64()
            };

            var count = reader.ReadInt32();

            for (var i = 0; i < count; i++)
            {
                var locator = Locator.Deserialize(reader);

                scheme._map[locator.Id] = locator;

                //Do not prepare the locator yet
            }

            return scheme;
        }

        public Locator this[long id] => _map[id];

        public Locator Create(string name, int structureType, DataType keyDataType, DataType recordDataType, Type keyType, Type recordType)
        {
            var id = ObtainPathId();

            var locator = new Locator(id, name, structureType, keyDataType, recordDataType, keyType, recordType);

            _map[id] = locator;

            return locator;
        }

        public int Count => _map.Count;

        public IEnumerator<KeyValuePair<long, Locator>> GetEnumerator()
        {
            return _map.Select(s => new KeyValuePair<long, Locator>(s.Key, s.Value)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}