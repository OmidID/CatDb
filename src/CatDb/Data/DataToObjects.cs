// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using System.Reflection;
using CatDb.General.Extensions;

namespace CatDb.Data;

// IData = object. To: cast object → T, extract fields to object[]. From: fill T from object[], box to object.
public class DataToObjects : IToObjects<IData>
{
    private readonly Func<IData, object[]> _to;
    private readonly Func<object[], IData> _from;

    private readonly Type _type;
    private readonly Func<Type, MemberInfo, int>? _membersOrder;

    public DataToObjects(Type type, Func<Type, MemberInfo, int>? membersOrder = null)
    {
        if (!DataType.IsPrimitiveType(type) && !type.HasDefaultConstructor())
            throw new NotSupportedException("No default constructor.");
        if (!DataTypeUtils.IsAllPrimitive(type))
            throw new NotSupportedException("Not all types are primitive.");

        _type         = type;
        _membersOrder = membersOrder;

        _to   = CreateToMethod().Compile();
        _from = CreateFromMethod().Compile();
    }

    public Expression<Func<IData, object[]>> CreateToMethod()
    {
        var data = Expression.Parameter(typeof(object), "data");
        var d    = Expression.Variable(_type, "d");

        var body = Expression.Block(new[] { d },
            Expression.Assign(d, Expression.Convert(data, _type)),
            ValueToObjectsHelper.ToObjects(d, _membersOrder));

        return Expression.Lambda<Func<IData, object[]>>(body, data);
    }

    public Expression<Func<object[], IData>> CreateFromMethod()
    {
        var objectArray = Expression.Parameter(typeof(object[]), "item");
        var data        = Expression.Variable(_type);
        var list        = new List<Expression>();

        if (!DataType.IsPrimitiveType(_type))
            list.Add(Expression.Assign(data, Expression.New(_type)));

        list.Add(ValueToObjectsHelper.FromObjects(data, objectArray, _membersOrder));
        // Box T → object
        list.Add(Expression.Convert(data, typeof(object)));

        var body = Expression.Block(typeof(object), new[] { data }, list);
        return Expression.Lambda<Func<object[], IData>>(body, objectArray);
    }

    public object[] To(IData value1)    => _to(value1);
    public IData    From(object[] value2) => _from(value2);
}
