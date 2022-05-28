using CatDb.Data;

namespace CatDb.Database
{
    public class XFile : XStream
    {
        internal XFile(ITable<IData, IData> table)
            : base(table)
        {
        }
    }
}
