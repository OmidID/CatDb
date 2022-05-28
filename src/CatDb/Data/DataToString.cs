using System.Linq.Expressions;
using System.Reflection;

namespace CatDb.Data
{
    public class DataToString : IToString<IData>
    {
        private readonly Func<IData, string> _to;
        private readonly Func<string, IData> _from;

        private readonly Type _type;
        private readonly int _stringBuilderCapacity;
        private readonly IFormatProvider[] _providers;
        private readonly char[] _delimiters;
        private readonly Func<Type, MemberInfo, int> _membersOrder;

        public DataToString(Type type, int stringBuilderCapacity, IFormatProvider[] providers, char[] delimiters, Func<Type, MemberInfo, int> membersOrder = null)
        {
            _type = type;
            _stringBuilderCapacity = stringBuilderCapacity;
            var typeCount = DataType.IsPrimitiveType(type) ? 1 : DataTypeUtils.GetPublicMembers(type, membersOrder).Count();
            if (providers.Length != typeCount)
                throw new ArgumentException("providers.Length != dataType.Length");

            _providers = providers;
            _delimiters = delimiters;
            _membersOrder = membersOrder;

            _to = CreateToMethod().Compile();
            _from = CreateFromMethod().Compile();
        }

        public DataToString(Type type, int stringBuilderCapacity, char[] delimiters, Func<Type, MemberInfo, int> membersOrder = null)
            : this(type, stringBuilderCapacity, ValueToStringHelper.GetDefaultProviders(type, membersOrder), delimiters, membersOrder)
        {
        }

        public DataToString(Type type, Func<Type, MemberInfo, int> membersOrder = null)
            : this(type, 16, new[] { ';' }, membersOrder)
        {
        }

        public Expression<Func<IData, string>> CreateToMethod()
        {
            var data = Expression.Parameter(typeof(IData), "data");
            var d = Expression.Variable(typeof(Data<>).MakeGenericType(_type), "d");

            var list = new List<Expression>
            {
                Expression.Assign(d, Expression.Convert(data, typeof(Data<>).MakeGenericType(_type))),
                ValueToStringHelper.CreateToStringBody(d.Value(), _stringBuilderCapacity, _providers, _delimiters[0],
                    _membersOrder)
            };

            var body = Expression.Block(new[] { d }, list);

            return Expression.Lambda<Func<IData, string>>(body, data);
        }

        public Expression<Func<string, IData>> CreateFromMethod()
        {
            var stringParam = Expression.Parameter(typeof(string), "item");
            var list = new List<Expression>();

            var data = Expression.Variable(typeof(Data<>).MakeGenericType(_type), "d");

            list.Add(Expression.Assign(data, Expression.New(data.Type.GetConstructor(new Type[] { }))));

            if (!DataType.IsPrimitiveType(_type))
                list.Add(Expression.Assign(data.Value(), Expression.New(_type.GetConstructor(new Type[] { }))));

            list.Add(ValueToStringHelper.CreateParseBody(data.Value(), stringParam, _providers, _delimiters, _membersOrder));
            list.Add(Expression.Label(Expression.Label(typeof(Data<>).MakeGenericType(_type)), data));

            var body = Expression.Block(new[] { data }, list);

            return Expression.Lambda<Func<string, IData>>(body, new[] { stringParam });
        }

        public string To(IData value1)
        {
            return _to(value1);
        }

        public IData From(string value2)
        {
            return _from(value2);
        }
    }
}
