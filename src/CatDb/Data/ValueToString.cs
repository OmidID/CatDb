using System.Text;
using CatDb.General.Extensions;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace CatDb.Data
{
    public class ValueToString<T> : IToString<T>
    {
        private readonly Func<T, string> _to;
        private readonly Func<string, T> _from;

        private readonly Type _type;
        private readonly int _stringBuilderCapacity;
        private readonly IFormatProvider[] _providers;
        private readonly char[] _delimiters;
        private readonly Func<Type, MemberInfo, int> _membersOrder;

        public ValueToString(int stringBuilderCapacity, IFormatProvider[] providers, char[] delimiters, Func<Type, MemberInfo, int> membersOrder = null)
        {
            if (!DataType.IsPrimitiveType(typeof(T)) && !typeof(T).HasDefaultConstructor())
                throw new NotSupportedException("No default constructor.");

            var isSupported = DataTypeUtils.IsAllPrimitive(typeof(T));
            if (!isSupported)
                throw new NotSupportedException("Not all types are primitive.");

            var countOfType = DataType.IsPrimitiveType(typeof(T)) ? 1 : DataTypeUtils.GetPublicMembers(typeof(T), membersOrder).Count();

            if (providers.Length != countOfType)
                throw new ArgumentException("providers.Length != dataType.Length");

            _type = typeof(T);
            _membersOrder = membersOrder;
            _stringBuilderCapacity = stringBuilderCapacity;
            _providers = providers;
            _delimiters = delimiters;

            _to = CreateToMethod().Compile();
            _from = CreateFromMethod().Compile();
        }

        public ValueToString(int stringBuilderCapacity, char[] delimiters, Func<Type, MemberInfo, int> membersOrder = null)
            : this(stringBuilderCapacity, ValueToStringHelper.GetDefaultProviders(typeof(T), membersOrder), delimiters, membersOrder)
        {
        }

        public ValueToString(Func<Type, MemberInfo, int> membersOrder = null)
            : this(16, new[] { ';' }, membersOrder)
        {
        }

        public Expression<Func<T, string>> CreateToMethod()
        {
            var item = Expression.Parameter(typeof(T));

            return Expression.Lambda<Func<T, string>>(ValueToStringHelper.CreateToStringBody(item, _stringBuilderCapacity, _providers, _delimiters[0], _membersOrder), new[] { item });
        }

        public Expression<Func<string, T>> CreateFromMethod()
        {
            var stringParam = Expression.Parameter(typeof(string), "item");
            var list = new List<Expression>();

            var item = Expression.Variable(_type);

            if (!DataType.IsPrimitiveType(_type))
                list.Add(Expression.Assign(item, Expression.New(_type.GetConstructor(new Type[] { }))));

            list.Add(ValueToStringHelper.CreateParseBody(item, stringParam, _providers, _delimiters, _membersOrder));
            list.Add(Expression.Label(Expression.Label(_type), item));

            var body = Expression.Block(new[] { item }, list);

            return Expression.Lambda<Func<string, T>>(body, new[] { stringParam });
        }

        public string To(T value1)
        {
            return _to(value1);
        }

        public T From(string value2)
        {
            return _from(value2);
        }
    }

    public static class ValueToStringHelper
    {
        public static Expression CreateToStringBody(Expression item, int stringBuilderCapacity, IFormatProvider[] providers, char delimiter, Func<Type, MemberInfo, int> membersOrder)
        {
            var stringBuilder = Expression.Variable(typeof(StringBuilder));

            if (DataType.IsPrimitiveType(item.Type) || DataTypeUtils.GetPublicMembers(item.Type, membersOrder).Count() == 1)
            {
                var member = DataType.IsPrimitiveType(item.Type) ? item : Expression.PropertyOrField(item, DataTypeUtils.GetPublicMembers(item.Type, membersOrder).First().Name);

                MethodCallExpression callToString;

                if (member.Type == typeof(byte[]))
                {
                    var toHexMethod = typeof(ByteArrayExtensions).GetMethod("ToHex", new[] { typeof(byte[]) });
                    callToString = Expression.Call(toHexMethod, member);
                }
                else if (member.Type == typeof(TimeSpan))
                {
                    var toStringProvider = member.Type.GetMethod("ToString", new[] { typeof(String), typeof(IFormatProvider) });
                    callToString = Expression.Call(member, toStringProvider, Expression.Constant(null, typeof(String)), Expression.Constant(providers[0], typeof(IFormatProvider)));
                }
                else
                {
                    var toStringProvider = member.Type.GetMethod("ToString", new[] { typeof(IFormatProvider) });
                    callToString = Expression.Call(member, toStringProvider, Expression.Constant(providers[0], typeof(IFormatProvider)));
                }

                return Expression.Label(Expression.Label(typeof(string)), member.Type == typeof(string) ? member : callToString);
            }

            var list = new List<Expression>
            {
                Expression.Assign(stringBuilder, Expression.New(stringBuilder.Type.GetConstructor(new[] { typeof(int) }), Expression.Constant(stringBuilderCapacity)))
            };

            var i = 0;
            foreach (var member in DataTypeUtils.GetPublicMembers(item.Type, membersOrder))
            {
                list.Add(GetAppendCommand(Expression.PropertyOrField(item, member.Name), stringBuilder, providers[i]));

                if (i < DataTypeUtils.GetPublicMembers(item.Type, membersOrder).Count() - 1)
                    list.Add(Expression.Call(stringBuilder, typeof(StringBuilder).GetMethod("Append", new[] { typeof(char) }), Expression.Constant(delimiter)));
                i++;
            }

            list.Add(Expression.Label(Expression.Label(typeof(string)), Expression.Call(stringBuilder, typeof(object).GetMethod("ToString"))));

            return Expression.Block(new[] { stringBuilder }, list);
        }

        private static Expression GetAppendCommand(Expression member, ParameterExpression stringBuilder, IFormatProvider provider)
        {
            MethodCallExpression callToString;

            if (member.Type == typeof(byte[]))
            {
                var toHexMethod = typeof(ByteArrayExtensions).GetMethod("ToHex", new[] { typeof(byte[]) });
                callToString = Expression.Call(toHexMethod, member);
            }
            else if (member.Type == typeof(TimeSpan))
            {
                var toStringProvider = member.Type.GetMethod("ToString", new[] { typeof(String), typeof(IFormatProvider) });
                callToString = Expression.Call(member, toStringProvider, Expression.Constant(null, typeof(String)), Expression.Constant(provider, typeof(IFormatProvider)));
            }
            else
            {
                var toStringProvider = member.Type.GetMethod("ToString", new[] { typeof(IFormatProvider) });
                callToString = Expression.Call(member, toStringProvider, Expression.Constant(provider, typeof(IFormatProvider)));
            }

            var apendMethod = typeof(StringBuilder).GetMethod("Append", new[] { typeof(String) });
            var callAppend = Expression.Call(stringBuilder, apendMethod, member.Type == typeof(string) ? member : callToString);

            return callAppend;
        }

        public static Expression CreateParseBody(Expression item, ParameterExpression stringParam, IFormatProvider[] providers, char[] delimiters, Func<Type, MemberInfo, int> membersOrder)
        {
            var array = Expression.Variable(typeof(string[]), "array");

            if (DataType.IsPrimitiveType(item.Type) || DataTypeUtils.GetPublicMembers(item.Type, membersOrder).Count() == 1)
            {
                var member = DataType.IsPrimitiveType(item.Type) ? item : Expression.PropertyOrField(item, DataTypeUtils.GetPublicMembers(item.Type, membersOrder).First().Name);

                Expression value;

                if (member.Type == typeof(String))
                {
                    value = stringParam;
                }
                else if (member.Type == typeof(byte[]))
                {
                    var hexParse = typeof(StringExtensions).GetMethod("ParseHex", new[] { typeof(string) });
                    value = Expression.Call(hexParse, stringParam);
                }
                else if (member.Type == typeof(char))
                {
                    var parseMethod = member.Type.GetMethod("Parse", new[] { typeof(string) });
                    value = Expression.Call(parseMethod, stringParam);
                }
                else if (member.Type == typeof(bool))
                {
                    var parseMethod = member.Type.GetMethod("Parse");
                    value = Expression.Call(parseMethod, stringParam);
                }
                else
                {
                    var parseMethod = member.Type.GetMethod("Parse", new[] { typeof(string), typeof(IFormatProvider) });
                    value = Expression.Call(parseMethod, stringParam, Expression.Constant(providers[0], typeof(IFormatProvider)));
                }

                return Expression.Assign(member, value);
            }

            var list = new List<Expression>
            {
                Expression.Assign(array, Expression.Call(stringParam, typeof(string).GetMethod("Split", new[] { typeof(char[]) }), new Expression[] { Expression.Constant(delimiters) }))
            };

            var i = 0;
            foreach (var member in DataTypeUtils.GetPublicMembers(item.Type, membersOrder))
                list.Add(GetParseCommand(Expression.PropertyOrField(item, member.Name), i, array, providers[i++]));

            return Expression.Block(new[] { array }, list);
        }

        private static Expression GetParseCommand(Expression member, int index, ParameterExpression stringArray, IFormatProvider provider)
        {
            var sValue = Expression.ArrayAccess(stringArray, Expression.Constant(index));
            Expression value;

            if (member.Type == typeof(String))
            {
                value = sValue;
            }
            else if (member.Type == typeof(byte[]))
            {
                var hexParse = typeof(StringExtensions).GetMethod("ParseHex", new[] { typeof(string) });
                value = Expression.Call(hexParse, sValue);
            }
            else if (member.Type == typeof(char))
            {
                var parseMethod = member.Type.GetMethod("Parse", new[] { typeof(string) });
                value = Expression.Call(parseMethod, sValue);
            }
            else if (member.Type == typeof(bool))
            {
                var parseMethod = member.Type.GetMethod("Parse");
                value = Expression.Call(parseMethod, sValue);
            }
            else
            {
                var parseMethod = member.Type.GetMethod("Parse", new[] { typeof(string), typeof(IFormatProvider) });
                value = Expression.Call(parseMethod, sValue, Expression.Constant(provider, typeof(IFormatProvider)));
            }

            return Expression.Assign(member, value);
        }

        public static IFormatProvider[] GetDefaultProviders(Type type, Func<Type, MemberInfo, int> membersOrder = null)
        {
            if (DataType.IsPrimitiveType(type))
                return new[] { GetDefaultProvider(type) };

            var providers = new List<IFormatProvider>();
            foreach (var member in DataTypeUtils.GetPublicMembers(type, membersOrder))
                providers.Add(GetDefaultProvider(member.GetPropertyOrFieldType()));

            return providers.ToArray();
        }

        public static IFormatProvider GetDefaultProvider(Type type)
        {
            if (!DataType.IsPrimitiveType(type))
                throw new NotSupportedException(type.ToString());

            if (type == typeof(float) ||
                type == typeof(double) ||
                type == typeof(decimal))
            {
                var numberFormat = new NumberFormatInfo
                {
                    CurrencyDecimalSeparator = "."
                };

                return numberFormat;
            }
            else if (type == typeof(DateTime) || type == typeof(TimeSpan))
            {
                var dateTimeFormat = new DateTimeFormatInfo
                {
                    DateSeparator = "-",
                    TimeSeparator = ":",
                    ShortDatePattern = "yyyy-MM-dd",
                    ShortTimePattern = "HH:mm:ss.fff"
                };
                dateTimeFormat.LongDatePattern = dateTimeFormat.ShortDatePattern;
                dateTimeFormat.LongTimePattern = dateTimeFormat.ShortTimePattern;

                return dateTimeFormat;
            }
            else
                return null;
        }
    }
}
