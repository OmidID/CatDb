using System.Linq.Expressions;
using System.Reflection;

namespace CatDb.Data
{
    public class DataEqualityComparer : IEqualityComparer<IData>
    {
        private readonly Func<IData, IData, bool> _equals;
        private readonly Func<IData, int> _getHashCode;

        private readonly Type _type;
        private readonly Func<Type, MemberInfo, int> _membersOrder;
        private readonly CompareOption[] _compareOptions;

        public DataEqualityComparer(Type type, CompareOption[] compareOptions, Func<Type, MemberInfo, int> membersOrder = null)
        {
            _type = type;
            CompareOption.CheckCompareOptions(type, compareOptions, membersOrder);
            _compareOptions = compareOptions;
            _membersOrder = membersOrder;

            _equals = CreateEqualsMethod().Compile();
            _getHashCode = CreateGetHashCodeMethod().Compile();
        }

        public DataEqualityComparer(Type type, Func<Type, MemberInfo, int> membersOrder = null)
            : this(type, CompareOption.GetDefaultCompareOptions(type, membersOrder), membersOrder)
        {
        }

        public Expression<Func<IData, IData, bool>> CreateEqualsMethod()
        {
            var x = Expression.Parameter(typeof(IData));
            var y = Expression.Parameter(typeof(IData));
            var xValue = Expression.Variable(_type);
            var yValue = Expression.Variable(_type);

            var dataType = typeof(Data<>).MakeGenericType(_type);

            var body = Expression.Block(typeof(bool), new[] { xValue, yValue },
                    Expression.Assign(xValue, Expression.Convert(x, dataType).Value()),
                    Expression.Assign(yValue, Expression.Convert(y, dataType).Value()),
                    EqualityComparerHelper.CreateEqualsBody(xValue, yValue, _compareOptions, _membersOrder)
                );
            var lambda = Expression.Lambda<Func<IData, IData, bool>>(body, x, y);

            return lambda;
        }

        public Expression<Func<IData, int>> CreateGetHashCodeMethod()
        {
            var obj = Expression.Parameter(typeof(IData));
            var objValue = Expression.Variable(_type);

            var dataType = typeof(Data<>).MakeGenericType(_type);

            var body = Expression.Block(typeof(int), new[] { objValue },
                Expression.Assign(objValue, Expression.Convert(obj, dataType).Value()),
                EqualityComparerHelper.CreateGetHashCodeBody(objValue, _membersOrder)
                );
            var lambda = Expression.Lambda<Func<IData, int>>(body, obj);

            return lambda;
        }

        public bool Equals(IData x, IData y)
        {
            return _equals(x, y);
        }

        public int GetHashCode(IData obj)
        {
            return _getHashCode(obj);
        }
    }
}
