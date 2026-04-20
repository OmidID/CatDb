using CatDb.Data;

namespace CatDb.Database;

public class XFile(ITable<IData, IData> table) : XStream(table)
{
}
