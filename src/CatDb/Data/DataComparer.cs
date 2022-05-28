using System.Linq.Expressions;
using System.Reflection;

namespace CatDb.Data
{
    public class DataComparer : IComparer<IData>
    {
        private readonly Func<IData, IData, int> _compare;

        private readonly Type _type;
        private readonly Type _dataType;
        private readonly CompareOption[] _compareOptions;
        private readonly Func<Type, MemberInfo, int> _membersOrder;

        public DataComparer(Type type, CompareOption[] compareOptions, Func<Type, MemberInfo, int> membersOrder = null)
        {
            _type = type;
            _dataType = typeof(Data<>).MakeGenericType(type);

            CompareOption.CheckCompareOptions(type, compareOptions, membersOrder);
            _compareOptions = compareOptions;
            _membersOrder = membersOrder;

            _compare = CreateCompareMethod().Compile();
        }

        public DataComparer(Type type, Func<Type, MemberInfo, int> membersOrder = null)
            : this(type, CompareOption.GetDefaultCompareOptions(type, membersOrder), membersOrder)
        {
        }

        public Expression<Func<IData, IData, int>> CreateCompareMethod()
        {
            var x = Expression.Parameter(typeof(IData));
            var y = Expression.Parameter(typeof(IData));

            var list = new List<Expression>();
            var parameters = new List<ParameterExpression>();

            var value1 = Expression.Variable(_type, "value1");
            parameters.Add(value1);
            list.Add(Expression.Assign(value1, Expression.Convert(x, _dataType).Value()));

            var value2 = Expression.Variable(_type, "value2");
            parameters.Add(value2);
            list.Add(Expression.Assign(value2, Expression.Convert(y, _dataType).Value()));

            return Expression.Lambda<Func<IData, IData, int>>(ComparerHelper.CreateComparerBody(list, parameters, value1, value2, _compareOptions, _membersOrder), x, y);
        }

        public int Compare(IData x, IData y)
        {
            return _compare(x, y);
        }
    }
}
