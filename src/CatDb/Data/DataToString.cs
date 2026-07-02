// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using System.Reflection;

namespace CatDb.Data;

// IData = object. To: cast object → T, convert to string. From: parse string → T, box to object.
public class DataToString : IToString<IData>
{
    private readonly Func<IData, string> _to;
    private readonly Func<string, IData> _from;

    private readonly Type _type;
    private readonly int _stringBuilderCapacity;
    private readonly IFormatProvider[] _providers;
    private readonly char[] _delimiters;
    private readonly Func<Type, MemberInfo, int>? _membersOrder;

    public DataToString(Type type, int stringBuilderCapacity, IFormatProvider[] providers, char[] delimiters, Func<Type, MemberInfo, int>? membersOrder = null)
    {
        _type = type;
        _stringBuilderCapacity = stringBuilderCapacity;
        var typeCount = DataType.IsPrimitiveType(type) ? 1 : DataTypeUtils.GetPublicMembers(type, membersOrder).Count();
        if (providers.Length != typeCount)
            throw new ArgumentException("providers.Length != dataType.Length");

        _providers = providers;
        _delimiters = delimiters;
        _membersOrder = membersOrder;

        _to   = CreateToMethod().Compile();
        _from = CreateFromMethod().Compile();
    }

    public DataToString(Type type, int stringBuilderCapacity, char[] delimiters, Func<Type, MemberInfo, int>? membersOrder = null)
        : this(type, stringBuilderCapacity, ValueToStringHelper.GetDefaultProviders(type, membersOrder), delimiters, membersOrder)
    {
    }

    public DataToString(Type type, Func<Type, MemberInfo, int>? membersOrder = null)
        : this(type, 16, new[] { ';' }, membersOrder)
    {
    }

    public Expression<Func<IData, string>> CreateToMethod()
    {
        var data = Expression.Parameter(typeof(object), "data");
        var d    = Expression.Variable(_type, "d");

        // Cast object → T, then serialize T's fields
        var list = new List<Expression>
        {
            Expression.Assign(d, Expression.Convert(data, _type)),
            ValueToStringHelper.CreateToStringBody(d, _stringBuilderCapacity, _providers, _delimiters[0], _membersOrder)
        };

        return Expression.Lambda<Func<IData, string>>(Expression.Block(new[] { d }, list), data);
    }

    public Expression<Func<string, IData>> CreateFromMethod()
    {
        var stringParam = Expression.Parameter(typeof(string), "item");
        var data        = Expression.Variable(_type, "d");
        var list        = new List<Expression>();

        // For complex types allocate a new instance; for primitives leave as default
        if (!DataType.IsPrimitiveType(_type))
            list.Add(Expression.Assign(data, Expression.New(_type)));

        list.Add(ValueToStringHelper.CreateParseBody(data, stringParam, _providers, _delimiters, _membersOrder));
        // Box T → object
        list.Add(Expression.Convert(data, typeof(object)));

        return Expression.Lambda<Func<string, IData>>(
            Expression.Block(typeof(object), new[] { data }, list), stringParam);
    }

    public string To(IData value1)  => _to(value1);
    public IData From(string value2) => _from(value2);
}
