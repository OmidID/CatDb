// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using System.Reflection;

namespace CatDb.Data;

public class DataTransformer<T> : ITransformer<T, IData>
{
    private readonly Func<T, IData> _to;
    private readonly Func<IData, T> _from;

    private readonly Type _type1;
    private readonly Type _type2;
    private readonly Func<Type, MemberInfo, int>? _membersOrder1;
    private readonly Func<Type, MemberInfo, int>? _membersOrder2;

    public DataTransformer(Type type2, Func<Type, MemberInfo, int>? membersOrder1 = null, Func<Type, MemberInfo, int>? membersOrder2 = null)
    {
        if (!TransformerHelper.CheckCompatible(typeof(T), type2, new HashSet<Type>(), membersOrder1, membersOrder2))
            throw new ArgumentException($"Type {typeof(T)} is not compatible with {type2}");

        _type1         = typeof(T);
        _type2         = type2;
        _membersOrder1 = membersOrder1;
        _membersOrder2 = membersOrder2;

        _to   = CreateToMethod().Compile();
        _from = CreateFromMethod().Compile();
    }

    // T → object: copy fields into a T2 instance, return as object (no Data<T2> wrapper).
    public Expression<Func<T, IData>> CreateToMethod()
    {
        var value = Expression.Parameter(_type1);

        if (TransformerHelper.IsEqualsTypes(_type1, _type2))
        {
            // Same type — just box/cast the value to object
            return Expression.Lambda<Func<T, IData>>(
                Expression.Convert(value, typeof(object)), value);
        }
        else
        {
            // Different types — allocate T2, copy fields, return as object
            var t2   = Expression.Variable(_type2);
            var list = new List<Expression>
            {
                Expression.Assign(t2, Expression.New(_type2)),
                TransformerHelper.BuildBody(t2, value, _membersOrder1, _membersOrder2),
                Expression.Convert(t2, typeof(object))
            };
            return Expression.Lambda<Func<T, IData>>(
                Expression.Block(typeof(object), new[] { t2 }, list), value);
        }
    }

    // object → T: cast object to T2, copy fields into T1.
    public Expression<Func<IData, T>> CreateFromMethod()
    {
        var idata = Expression.Parameter(typeof(object));

        if (TransformerHelper.IsEqualsTypes(_type1, _type2))
        {
            // Same type — just cast/unbox object to T1
            return Expression.Lambda<Func<IData, T>>(
                Expression.Convert(idata, _type1), idata);
        }
        else
        {
            // Different types — cast to T2, copy fields into T1
            var dataVal = Expression.Variable(_type2);
            var result  = Expression.Variable(_type1);
            var list    = new List<Expression>
            {
                Expression.Assign(dataVal, Expression.Convert(idata, _type2)),
                TransformerHelper.BuildBody(result, dataVal, _membersOrder2, _membersOrder1),
                Expression.Label(Expression.Label(_type1), result)
            };
            return Expression.Lambda<Func<IData, T>>(
                Expression.Block(_type1, new[] { dataVal, result }, list), idata);
        }
    }

    public IData To(T value1) => _to(value1);
    public T From(IData value2) => _from(value2);
}
