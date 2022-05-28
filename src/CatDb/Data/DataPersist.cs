using CatDb.General.Persist;
using System.Linq.Expressions;
using System.Reflection;

namespace CatDb.Data
{
    public class DataPersist : IPersist<IData>
    {
        private readonly Action<BinaryWriter, IData> _write;
        private readonly Func<BinaryReader, IData> _read;

        private readonly Type _type;
        private readonly Func<Type, MemberInfo, int> _membersOrder;
        private readonly AllowNull _allowNull;

        public DataPersist(Type type, Func<Type, MemberInfo, int> membersOrder = null, AllowNull allowNull = AllowNull.None)
        {
            _type = type;
            _membersOrder = membersOrder;
            _allowNull = allowNull;

            _write = CreateWriteMethod().Compile();
            _read = CreateReadMethod().Compile();
        }

        public void Write(BinaryWriter writer, IData item)
        {
            _write(writer, item);
        }

        public IData Read(BinaryReader reader)
        {
            return _read(reader);
        }

        public Expression<Action<BinaryWriter, IData>> CreateWriteMethod()
        {
            var writer = Expression.Parameter(typeof(BinaryWriter), "writer");
            var idata = Expression.Parameter(typeof(IData), "idata");

            var dataType = typeof(Data<>).MakeGenericType(_type);
            var dataValue = Expression.Variable(_type, "dataValue");

            var assign = Expression.Assign(dataValue, Expression.Convert(idata, dataType).Value());

            return Expression.Lambda<Action<BinaryWriter, IData>>(Expression.Block(new[] { dataValue }, assign, PersistHelper.CreateWriteBody(dataValue, writer, _membersOrder, _allowNull)), writer, idata);
        }

        public Expression<Func<BinaryReader, IData>> CreateReadMethod()
        {
            var reader = Expression.Parameter(typeof(BinaryReader), "reader");

            var dataType = typeof(Data<>).MakeGenericType(_type);

            return Expression.Lambda<Func<BinaryReader, IData>>(
                    Expression.Label(Expression.Label(dataType), Expression.New(dataType.GetConstructor(new[] { _type }), PersistHelper.CreateReadBody(reader, _type, _membersOrder, _allowNull))),
                    reader
                );
        }
    }
}