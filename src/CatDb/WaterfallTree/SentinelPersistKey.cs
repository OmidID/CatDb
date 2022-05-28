using CatDb.General.Persist;
using CatDb.Data;

namespace CatDb.WaterfallTree
{
    public class SentinelPersistKey : IPersist<IData>
    {
        public static readonly SentinelPersistKey Instance = new SentinelPersistKey();

        public void Write(BinaryWriter writer, IData item)
        {
        }

        public IData Read(BinaryReader reader)
        {
            return null;
        }
    }
}
