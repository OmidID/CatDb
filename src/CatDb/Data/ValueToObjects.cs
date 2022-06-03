using System.Linq.Expressions;
using System.Reflection;
using CatDb.General.Extensions;

namespace CatDb.Data
{
    public class ValueToObjects<T> : IToObjects<T>
    {
        private readonly Func<object[], T> _from;
        private readonly Func<T, object[]> _to;

        private readonly Type _type;
        private readonly Func<Type, MemberInfo, int> _membersOrder;

        public ValueToObjects(Func<Type, MemberInfo, int> membersOrder = null)
        {
            if (!DataType.IsPrimitiveType(typeof(T)) && !typeof(T).HasDefaultConstructor())
                throw new NotSupportedException("No default constructor.");

            var isSupported = DataTypeUtils.IsAllPrimitive(typeof(T));
            if (!isSupported)
                throw new NotSupportedException("Not all types are primitive.");

            _type = typeof(T);
            _membersOrder = membersOrder;

            _to = CreateToMethod().Compile();
            _from = CreateFromMethod().Compile();
        }

        public Expression<Func<T, object[]>> CreateToMethod()
        {
            var item = Expression.Parameter(_type);

            return Expression.Lambda<Func<T, object[]>>(ValueToObjectsHelper.ToObjects(item, _membersOrder), item);
        }

        public Expression<Func<object[], T>> CreateFromMethod()
        {
            var objectArray = Expression.Parameter(typeof(object[]), "item");
            var item = Expression.Variable(_type);
            var list = new List<Expression>();

            if (!DataType.IsPrimitiveType(_type))
                list.Add(Expression.Assign(item, Expression.New(item.Type.GetConstructor(new Type[] { }))));

            list.Add(ValueToObjectsHelper.FromObjects(item, objectArray, _membersOrder));
            list.Add(Expression.Label(Expression.Label(typeof(T)), item));

            var body = Expression.Block(typeof(T), new[] { item }, list);

            return Expression.Lambda<Func<object[], T>>(body, objectArray);
        }

        public object[] To(T value1)
        {
            return _to(value1);
        }

        public T From(object[] value2)
        {
            return _from(value2);
        }
    }

    public static class ValueToObjectsHelper
    {
        public static Expression ToObjects(Expression item, Func<Type, MemberInfo, int> membersOrder)
        {
            var types = DataType.IsPrimitiveType(item.Type) ? new[] { item.Type } : DataTypeUtils.GetPublicMembers(item.Type, membersOrder).Select(x => x.GetPropertyOrFieldType()).ToArray();

            if (types.Length == 1)
                return Expression.NewArrayInit(typeof(object), Expression.Convert(item, typeof(object)));

            var values = new Expression[types.Length];
            var i = 0;

            foreach (var member in DataTypeUtils.GetPublicMembers(item.Type, membersOrder))
                values[i++] = Expression.Convert(Expression.PropertyOrField(item, member.Name), typeof(object));

            return Expression.NewArrayInit(typeof(object), values);
        }

        public static Expression FromObjects(Expression item, ParameterExpression objectArray, Func<Type, MemberInfo, int> membersOrder)
        {
            var types = DataType.IsPrimitiveType(item.Type) ? new[] { item.Type } : DataTypeUtils.GetPublicMembers(item.Type, membersOrder).Select(x => x.GetPropertyOrFieldType()).ToArray();

            if (types.Length == 1)
                return Expression.Assign(item, Expression.Convert(Expression.ArrayAccess(objectArray, Expression.Constant(0, typeof(int))), types[0]));

            var list = new List<Expression>();
            var i = 0;
            foreach (var member in DataTypeUtils.GetPublicMembers(item.Type, membersOrder))
                list.Add(Expression.Assign(Expression.PropertyOrField(item, member.Name), Expression.Convert(Expression.ArrayAccess(objectArray, Expression.Constant(i, typeof(int))), types[i++])));

            return Expression.Block(list);
        }
    }
}