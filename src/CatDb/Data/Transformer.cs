using System.Linq.Expressions;
using System.Reflection;
using CatDb.General.Comparers;
using CatDb.General.Extensions;

namespace CatDb.Data
{
    public class Transformer<T1, T2> : ITransformer<T1, T2>
    {
        private readonly Func<T1, T2> _to;
        private readonly Func<T2, T1> _from;

        private readonly Type _type1;
        private readonly Type _type2;
        private readonly Func<Type, MemberInfo, int> _membersOrder1;
        private readonly Func<Type, MemberInfo, int> _membersOrder2;

        public Transformer(Func<Type, MemberInfo, int> membersOrder1 = null, Func<Type, MemberInfo, int> membersOrder2 = null)
        {
            if (!TransformerHelper.CheckCompatible(typeof(T1), typeof(T2), new HashSet<Type>(), membersOrder1, membersOrder2))
                throw new ArgumentException($"{typeof(T1)} not compatible with {typeof(T2).ToString()}");

            _type1 = typeof(T1);
            _type2 = typeof(T2);
            _membersOrder1 = membersOrder1;
            _membersOrder2 = membersOrder2;

            _to = CreateToMethod().Compile();
            _from = CreateFromMethod().Compile();
        }

        public Expression<Func<T1, T2>> CreateToMethod()
        {
            var value1 = Expression.Parameter(_type1);
            var value2 = Expression.Variable(_type2);

            var list = new List<Expression>();
            if (TransformerHelper.IsEqualsTypes(_type1, _type2))
                list.Add(Expression.Label(Expression.Label(_type1), value1));
            else
            {
                list.Add(TransformerHelper.BuildBody(value2, value1, _membersOrder2, _membersOrder1));
                list.Add(Expression.Label(Expression.Label(_type2), value2));
            }

            return Expression.Lambda<Func<T1, T2>>(TransformerHelper.IsEqualsTypes(_type1, _type2) ? list[0] : Expression.Block(typeof(T2), new[] { value2 }, list), value1);
        }

        public Expression<Func<T2, T1>> CreateFromMethod()
        {
            var value2 = Expression.Parameter(_type2);
            var value1 = Expression.Variable(_type1);

            var list = new List<Expression>();
            if (TransformerHelper.IsEqualsTypes(_type1, _type2))
                list.Add(Expression.Label(Expression.Label(_type2), value2));
            else
            {
                list.Add(TransformerHelper.BuildBody(value1, value2, _membersOrder1, _membersOrder2));
                list.Add(Expression.Label(Expression.Label(_type1), value1));
            }

            return Expression.Lambda<Func<T2, T1>>(TransformerHelper.IsEqualsTypes(_type1, _type2) ? list[0] : Expression.Block(typeof(T1), new[] { value1 }, list), value2);
        }


        public T2 To(T1 value1)
        {
            return _to(value1);
        }

        public T1 From(T2 value2)
        {
            return _from(value2);
        }
    }

    public static class TransformerHelper
    {
        public static Expression BuildBody(Expression value1, Expression value2, Func<Type, MemberInfo, int> membersOrder1, Func<Type, MemberInfo, int> membersOrder2)
        {
            var type1 = value1.Type;
            var type2 = value2.Type;

            if (type1 == typeof(Guid) || type2 == typeof(Guid))
                return Expression.Assign(value1,
                        type1 == typeof(Guid) ?
                        value2.Type == typeof(Guid) ? value2 : Expression.New(type1.GetConstructor(new[] { typeof(byte[]) }), value2) :
                            Expression.Call(value2, type2.GetMethod("ToByteArray"))
                    );

            if (type1.IsEnum || type2.IsEnum)
                return Expression.Assign(value1, Expression.Convert(value2, type1));

            if (IsEqualsTypes(type1, type2))
                return Expression.Assign(value1, value2);

            if (IsNumberType(type1) && IsNumberType(type2))
                return Expression.Assign(value1, Expression.Convert(value2, type1));

            if (type1.IsKeyValuePair())
            {
                var key = Expression.Variable(type1.GetGenericArguments()[0]);
                var value = Expression.Variable(type1.GetGenericArguments()[1]);

                return Expression.Assign(value1,
                    Expression.New((typeof(KeyValuePair<,>).MakeGenericType(key.Type, value.Type)).GetConstructor(new[] { key.Type, value.Type }),
                        Expression.Block(key.Type,
                            new[] { key },
                            BuildBody(key, Expression.PropertyOrField(value2, type2.IsKeyValuePair() ? "Key" : DataTypeUtils.GetPublicMembers(value2.Type, membersOrder2).First().Name), membersOrder1, membersOrder2),
                            Expression.Label(Expression.Label(key.Type), key)),
                        Expression.Block(value.Type,
                            new[] { value },
                            BuildBody(value, Expression.PropertyOrField(value2, type2.IsKeyValuePair() ? "Value" : DataTypeUtils.GetPublicMembers(value2.Type, membersOrder2).Last().Name), membersOrder1, membersOrder2),
                            Expression.Label(Expression.Label(value.Type), value))
                    ));
            }

            if (type1.IsList() || type1.IsArray)
            {
                var element = Expression.Variable(type1.IsArray ? type1.GetElementType() : type1.GetGenericArguments()[0]);

                var block = Expression.Block(new[] { element },
                    Expression.Assign(value1, Expression.New(value1.Type.GetConstructor(new[] { typeof(int) }), Expression.PropertyOrField(value2, type2.IsList() ? "Count" : "Length"))),
                    value2.For(i =>
                    {
                        return type2.IsList() ?
                            Expression.Call(value1, type1.GetMethod("Add"), BuildBody(element, value2.This(i), membersOrder1, membersOrder2)) :
                            Expression.Assign(Expression.ArrayAccess(value1, i), BuildBody(element, Expression.ArrayAccess(value2, i), membersOrder1, membersOrder2));
                    },
                    Expression.Label())
                    );

                return Expression.IfThenElse(Expression.NotEqual(value2, Expression.Constant(null)),
                    block,
                    Expression.Assign(value1, Expression.Constant(null, value1.Type)));
            }

            if (type1.IsDictionary())
            {
                if (!DataType.IsPrimitiveType(type1.GetGenericArguments()[0]) && type1.GetGenericArguments()[0] == typeof(Guid))
                    throw new NotSupportedException($"Dictionary<{type1.GetGenericArguments()[0]}, TValue>");

                var key = Expression.Variable(type1.GetGenericArguments()[0]);
                var value = Expression.Variable(type1.GetGenericArguments()[1]);

                var block = Expression.Block(new[] { key, value },
                    Expression.Assign(value1, type2.GetGenericArguments()[0] == typeof(byte[]) ?
                        Expression.New(type1.GetConstructor(new[] { typeof(int), typeof(IEqualityComparer<byte[]>) }), Expression.PropertyOrField(value2, "Count"), Expression.Field(null, typeof(BigEndianByteArrayEqualityComparer), "Instance")) :
                        Expression.New(type1.GetConstructor(new[] { typeof(int) }), Expression.PropertyOrField(value2, "Count"))),
                    value2.ForEach(current =>
                    Expression.Call(value1, type1.GetMethod("Add"),
                        BuildBody(key, Expression.Property(current, "Key"), membersOrder1, membersOrder2),
                        BuildBody(value, Expression.Property(current, "Value"), membersOrder1, membersOrder2)),
                    Expression.Label()
                    ));

                return Expression.IfThenElse(Expression.NotEqual(value2, Expression.Constant(null)),
                    block,
                    Expression.Assign(value1, Expression.Constant(null, value1.Type)));
            }

            if (type1.IsNullable())
            {
                var data1Var = Expression.Variable(value1.Type);
                var data2Var = Expression.Variable(value2.Type);

                new List<Expression>();

                var constructParam = Expression.PropertyOrField(data2Var, type2.IsNullable() ? "Value" : DataTypeUtils.GetPublicMembers(type2, membersOrder2).First().Name);

                var block = Expression.Block(new[] { data1Var, data2Var },
                        Expression.Assign(data2Var, value2),
                        Expression.Assign(data1Var, Expression.New(
                            type1.GetConstructor(new[] { type1.GetGenericArguments()[0] }),
                                constructParam.GetType() == type1.GetGenericArguments()[0] ?
                                constructParam :
                                Expression.Convert(constructParam, type1.GetGenericArguments()[0]))),
                        Expression.Assign(value1, data1Var)
                    );

                return Expression.IfThenElse(Expression.NotEqual(value2, Expression.Constant(null, type2)),
                        block,
                        Expression.Assign(value1, Expression.Constant(null, type1))
                    );
            }

            if (type1.IsClass || type1.IsStruct())
            {
                var data1Var = Expression.Variable(value1.Type);
                var data2Var = Expression.Variable(value2.Type);

                var list = new List<Expression> { Expression.Assign(data1Var, Expression.New(data1Var.Type)) };

                var members1 = DataTypeUtils.GetPublicMembers(value1.Type, membersOrder1).ToList();

                var members2 = new List<MemberInfo>();
                if (type2.IsKeyValuePair() || type2.IsNullable())
                {
                    if (type2.IsKeyValuePair())
                        members2.Add(type2.GetMember("Key")[0]);

                    members2.Add(type2.GetMember("Value")[0]);
                }
                else
                    members2 = DataTypeUtils.GetPublicMembers(value2.Type, membersOrder2).ToList();

                for (var i = 0; i < members1.Count; i++)
                    list.Add(BuildBody(Expression.PropertyOrField(data1Var, members1[i].Name), Expression.PropertyOrField(data2Var, members2[i].Name), membersOrder1, membersOrder2));

                list.Add(Expression.Assign(value1, data1Var));

                if ((type1.IsStruct() || type2.IsStruct()) && !type2.IsNullable())
                {
                    list.Insert(0, Expression.Assign(data2Var, value2));
                    list.Add(Expression.Label(Expression.Label(value1.Type), value1));
                    return Expression.Block(type1, new[] { data1Var, data2Var }, list);
                }

                return Expression.Block(type1, new[] { data2Var },
                    Expression.Assign(data2Var, value2),
                    Expression.IfThenElse(Expression.NotEqual(data2Var, Expression.Constant(null)),
                        Expression.Block(new[] { data1Var }, list),
                        Expression.Assign(value1, Expression.Constant(null, type1))),
                    Expression.Label(Expression.Label(type1), value1)
                    );
            }

            throw new NotSupportedException(type1.ToString());
        }

        public static bool CheckCompatible(Type type1, Type type2, HashSet<Type> cycleCheck, Func<Type, MemberInfo, int> membersOrder1 = null, Func<Type, MemberInfo, int> membersOrder2 = null)
        {
            if (type1 == typeof(Guid) || type1 == typeof(byte[]))
                return type2 == typeof(Guid) || type2 == typeof(byte[]);

            if (type1.IsEnum || type2.IsEnum)
                return (type1.IsEnum && type2.IsEnum) || (IsIntegerType(type1) || IsIntegerType(type2));

            if (DataType.IsPrimitiveType(type1))
                return (type1 == type2) || (IsNumberType(type1) && IsNumberType(type2));

            if (type1.IsArray)
                return CheckCompatible(type1.GetElementType(), type2.GetElementType(), cycleCheck, membersOrder1, membersOrder2);

            if (type1.IsList())
                return CheckCompatible(type1.GetGenericArguments()[0], type2.GetGenericArguments()[0], cycleCheck, membersOrder1, membersOrder2);

            if (type1.IsDictionary())
                return CheckCompatible(type1.GetGenericArguments()[0], type2.GetGenericArguments()[0], cycleCheck, membersOrder1, membersOrder2) && CheckCompatible(type1.GetGenericArguments()[1], type2.GetGenericArguments()[1], cycleCheck, membersOrder1, membersOrder2);

            if (type1.IsClass || type1.IsStruct())
            {
                var type1Slotes = new List<Type>();
                var type2Slotes = new List<Type>();

                if (type1.IsNullable())
                    type1Slotes.Add(type1.GetGenericArguments()[0]);

                if (type2.IsNullable())
                    type2Slotes.Add(type2.GetGenericArguments()[0]);

                if (type1.IsKeyValuePair())
                {
                    type1Slotes.Add(type1.GetGenericArguments()[0]);
                    type1Slotes.Add(type1.GetGenericArguments()[1]);
                }

                if (type2.IsKeyValuePair())
                {
                    type2Slotes.Add(type2.GetGenericArguments()[0]);
                    type2Slotes.Add(type2.GetGenericArguments()[1]);
                }

                foreach (var slote in DataTypeUtils.GetPublicMembers(type1, membersOrder1))
                    type1Slotes.Add(slote.GetPropertyOrFieldType());

                foreach (var slote in DataTypeUtils.GetPublicMembers(type2, membersOrder2))
                    type2Slotes.Add(slote.GetPropertyOrFieldType());

                if (type1Slotes.Count != type2Slotes.Count)
                    return false;

                for (var i = 0; i < type1Slotes.Count; i++)
                {
                    if (cycleCheck.Contains(type1Slotes[i]))
                        throw new NotSupportedException($"Type {type1Slotes[i]} has cycle declaration.");

                    cycleCheck.Add(type1Slotes[i]);
                    if (!CheckCompatible(type1Slotes[i], type2Slotes[i], cycleCheck, membersOrder1, membersOrder2))
                        return false;
                    cycleCheck.Remove(type1Slotes[i]);
                }

                return true;
            }

            throw new NotSupportedException(type2.ToString());
        }

        public static bool IsNumberType(Type type)
        {
            return IsIntegerType(type) || IsDecimalType(type);
        }

        public static bool IsIntegerType(Type type)
        {
            return type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(short) || type == typeof(ushort) || type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong);
        }

        public static bool IsDecimalType(Type type)
        {
            return type == typeof(float) || type == typeof(double) || type == typeof(decimal);
        }

        public static bool IsEqualsTypes(Type type1, Type type2)
        {
            if (type1.IsArray && type2.IsArray)
                return IsEqualsTypes(type1.GetElementType(), type2.GetElementType());

            if (type1.IsList() && type2.IsList())
                return IsEqualsTypes(type1.GetGenericArguments()[0], type2.GetGenericArguments()[0]);

            if ((type1.IsDictionary() && type2.IsDictionary()) || (type1.IsKeyValuePair() && type2.IsKeyValuePair()))
                return IsEqualsTypes(type1.GetGenericArguments()[0], type2.GetGenericArguments()[0]) && IsEqualsTypes(type1.GetGenericArguments()[1], type2.GetGenericArguments()[1]);

            if (type1 != type2)
                return false;

            return true;
        }
    }
}
