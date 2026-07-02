// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using System.Reflection;

namespace CatDb.Data;

public class DataEqualityComparer : IEqualityComparer<IData>
{
    private readonly Func<IData, IData, bool> _equals;
    private readonly Func<IData, int> _getHashCode;

    private readonly Type _type;
    private readonly Func<Type, MemberInfo, int>? _membersOrder;
    private readonly CompareOption[] _compareOptions;

    public DataEqualityComparer(Type type, CompareOption[] compareOptions, Func<Type, MemberInfo, int>? membersOrder = null)
    {
        _type           = type;
        _compareOptions = compareOptions;
        _membersOrder   = membersOrder;
        CompareOption.CheckCompareOptions(type, compareOptions, membersOrder);
        _equals      = CreateEqualsMethod().Compile();
        _getHashCode = CreateGetHashCodeMethod().Compile();
    }

    public DataEqualityComparer(Type type, Func<Type, MemberInfo, int>? membersOrder = null)
        : this(type, CompareOption.GetDefaultCompareOptions(type, membersOrder), membersOrder)
    {
    }

    public Expression<Func<IData, IData, bool>> CreateEqualsMethod()
    {
        var x = Expression.Parameter(typeof(object));
        var y = Expression.Parameter(typeof(object));

        // Cast object → T directly (no Data<T> intermediary)
        var xValue = Expression.Variable(_type);
        var yValue = Expression.Variable(_type);

        var body = Expression.Block(typeof(bool), new[] { xValue, yValue },
            Expression.Assign(xValue, Expression.Convert(x, _type)),
            Expression.Assign(yValue, Expression.Convert(y, _type)),
            EqualityComparerHelper.CreateEqualsBody(xValue, yValue, _compareOptions, _membersOrder));

        return Expression.Lambda<Func<IData, IData, bool>>(body, x, y);
    }

    public Expression<Func<IData, int>> CreateGetHashCodeMethod()
    {
        var obj      = Expression.Parameter(typeof(object));
        var objValue = Expression.Variable(_type);

        var body = Expression.Block(typeof(int), new[] { objValue },
            Expression.Assign(objValue, Expression.Convert(obj, _type)),
            EqualityComparerHelper.CreateGetHashCodeBody(objValue, _membersOrder));

        return Expression.Lambda<Func<IData, int>>(body, obj);
    }

    public new bool Equals(IData? x, IData? y) => _equals(x!, y!);
    public int GetHashCode(IData obj)       => _getHashCode(obj);
}
