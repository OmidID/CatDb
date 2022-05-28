using CatDb.Data;
using CatDb.General.Persist;
using System.Collections.Concurrent;

namespace CatDb.WaterfallTree
{
    public class TypeEngine
    {
        private static readonly ConcurrentDictionary<Type, TypeEngine> Map = new();

        public IComparer<IData> Comparer { get; set; }
        public IEqualityComparer<IData> EqualityComparer { get; set; }
        public IPersist<IData> Persist { get; set; }
        public IIndexerPersist<IData> IndexerPersist { get; set; }

        public TypeEngine()
        {
        }

        private static TypeEngine Create(Type type)
        {
            var descriptor = new TypeEngine
            {
                Persist = new DataPersist(type, null, AllowNull.OnlyMembers)
            };

            if (DataTypeUtils.IsAllPrimitive(type) || type == typeof(Guid))
            {
                descriptor.Comparer = new DataComparer(type);
                descriptor.EqualityComparer = new DataEqualityComparer(type);

                if (type != typeof(Guid))
                    descriptor.IndexerPersist = new DataIndexerPersist(type);
            }

            return descriptor;
        }

        public static TypeEngine Default(Type type)
        {
            return Map.GetOrAdd(type, Create(type));
        }
    }
}
