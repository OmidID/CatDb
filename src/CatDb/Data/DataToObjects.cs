using System.Linq.Expressions;
using System.Reflection;
using CatDb.General.Extensions;

namespace CatDb.Data
{
    public class DataToObjects : IToObjects<IData>
    {
        private readonly Func<IData, object[]> _to;
        private readonly Func<object[], IData> _from;

        private readonly Type _type;
        private readonly Func<Type, MemberInfo, int> _membersOrder;

        public DataToObjects(Type type, Func<Type, MemberInfo, int> membersOrder = null)
        {
            if (!DataType.IsPrimitiveType(type) && !type.HasDefaultConstructor())
                throw new NotSupportedException("No default constructor.");

            var isSupported = DataTypeUtils.IsAllPrimitive(type);
            if (!isSupported)
                throw new NotSupportedException("Not all types are primitive.");

            _type = type;
            _membersOrder = membersOrder;

            _to = CreateToMethod().Compile();
            _from = CreateFromMethod().Compile();
        }

        public Expression<Func<IData, object[]>> CreateToMethod()
        {
            var data = Expression.Parameter(typeof(IData), "data");

            var d = Expression.Variable(typeof(Data<>).MakeGenericType(_type), "d");
            var body = Expression.Block(new[] { d }, Expression.Assign(d, Expression.Convert(data, d.Type)), ValueToObjectsHelper.ToObjects(d.Value(), _membersOrder));

            return Expression.Lambda<Func<IData, object[]>>(body, data);
        }

        public Expression<Func<object[], IData>> CreateFromMethod()
        {
            var objectArray = Expression.Parameter(typeof(object[]), "item");
            var data = Expression.Variable(typeof(Data<>).MakeGenericType(_type));

            var list = new List<Expression> { Expression.Assign(data, Expression.New(data.Type.GetConstructor(new Type[] { }))) };

            if (!DataType.IsPrimitiveType(_type))
                list.Add(Expression.Assign(data.Value(), Expression.New(data.Value().Type.GetConstructor(new Type[] { }))));

            list.Add(ValueToObjectsHelper.FromObjects(data.Value(), objectArray, _membersOrder));
            list.Add(Expression.Label(Expression.Label(typeof(IData)), data));

            var body = Expression.Block(typeof(IData), new[] { data }, list);

            return Expression.Lambda<Func<object[], IData>>(body, objectArray);
        }

        public object[] To(IData value1)
        {
            return _to(value1);
        }

        public IData From(object[] value2)
        {
            return _from(value2);
        }
    }
}
