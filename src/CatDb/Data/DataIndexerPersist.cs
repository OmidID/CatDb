// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using System.Reflection;
using CatDb.General.Extensions;
using CatDb.General.Persist;

namespace CatDb.Data;

// Vertically-compressed IData (= object) column serializer.
// Store: Func<int, object> callback — unbox/cast object → T, write fields.
// Load:  Action<int, object> callback — deserialize fields into T[], box each to object.
// No Data<T> wrapper. For primitive types IndexerPersistHelper.SingleSlotCreateLoadBody
// (isData=false) produces the value directly; for complex types we use T[] as the staging
// array and pass isData=false so GetLoadPersistCall writes array[i].Field not array[i].Value.Field.
public class DataIndexerPersist : IIndexerPersist<IData>
{
    private readonly Action<BinaryWriter, Func<int, IData>, int> _store;
    private readonly Action<BinaryReader, Action<int, IData>, int> _load;

    private readonly Type _type;
    private readonly IIndexerPersist[] _persists;
    private readonly Func<Type, MemberInfo, int>? _membersOrder;

    public DataIndexerPersist(Type type, IIndexerPersist[] persists, Func<Type, MemberInfo, int>? membersOrder = null)
    {
        _type         = type;
        _persists     = persists;
        _membersOrder = membersOrder;

        _store = CreateStoreMethod().Compile();
        _load  = CreateLoadMethod().Compile();
    }

    public DataIndexerPersist(Type T, Func<Type, MemberInfo, int>? membersOrder = null)
        : this(T, IndexerPersistHelper.GetDefaultPersists(T, membersOrder), membersOrder)
    {
    }

    public Expression<Action<BinaryWriter, Func<int, IData>, int>> CreateStoreMethod()
    {
        var writer = Expression.Parameter(typeof(BinaryWriter), "writer");
        var values = Expression.Parameter(typeof(Func<int, IData>), "values");
        var count  = Expression.Parameter(typeof(int), "count");
        var idx    = Expression.Variable(typeof(int), "idx");

        // Invoke callback → object → cast/unbox to T (no Data<T> intermediary)
        var callValues = Expression.Convert(
            Expression.Call(values, values.Type.GetMethod("Invoke")!, idx),
            _type);

        var body   = IndexerPersistHelper.CreateStoreBody(_type, _persists, writer, callValues, idx, count, _membersOrder);
        return Expression.Lambda<Action<BinaryWriter, Func<int, IData>, int>>(body, writer, values, count);
    }

    public Expression<Action<BinaryReader, Action<int, IData>, int>> CreateLoadMethod()
    {
        var reader = Expression.Parameter(typeof(BinaryReader), "reader");
        var values = Expression.Parameter(typeof(Action<int, IData>), "func");
        var count  = Expression.Parameter(typeof(int), "count");

        Expression body;

        if (DataType.IsPrimitiveType(_type))
        {
            // Single-slot path: IndexerPersistHelper calls values(idx, value) where value is T.
            // We need values(idx, (object)value) — box T to object for Action<int,object>.
            // Build an adapter Action<int,T> that boxes and forwards to values(Action<int,object>).
            var adapterIdx = Expression.Parameter(typeof(int), "i");
            var adapterVal = Expression.Parameter(_type, "v");
            var adapter    = Expression.Lambda(
                Expression.Call(values, values.Type.GetMethod("Invoke")!,
                    adapterIdx, Expression.Convert(adapterVal, typeof(object))),
                adapterIdx, adapterVal);

            body = IndexerPersistHelper.SingleSlotCreateLoadBody(_type, false, adapter, reader, count, _persists);
        }
        else
        {
            // Multi-field path: use T[] staging array (not Data<T>[]).
            // Pre-allocate T instances, register with callback (pass as object reference),
            // then let CreateLoadBody fill each instance's fields from the reader.
            var array    = Expression.Variable(_type.MakeArrayType());
            var arrayCtor = _type.MakeArrayType().GetConstructor(new[] { typeof(int) })!;

            var invokeMethod = values.Type.GetMethod("Invoke")!;

            body = Expression.Block(new[] { array },
                Expression.Assign(array, Expression.New(arrayCtor, count)),
                array.For(i =>
                    Expression.Block(
                        // array[i] = new T()
                        Expression.Assign(Expression.ArrayAccess(array, i), Expression.New(_type)),
                        // values(i, (object)array[i])  — gives receiver a stable reference
                        Expression.Call(values, invokeMethod,
                            i, Expression.Convert(Expression.ArrayAccess(array, i), typeof(object)))),
                    Expression.Label(), count),
                // Fill array[i].Field values from reader (isData=false → no .Value indirection)
                IndexerPersistHelper.CreateLoadBody(_type, false, reader, array, count, _membersOrder, _persists));
        }

        return Expression.Lambda<Action<BinaryReader, Action<int, IData>, int>>(body, reader, values, count);
    }

    public void Store(BinaryWriter writer, Func<int, IData> values, int count) => _store(writer, values, count);
    public void Load(BinaryReader reader, Action<int, IData> values, int count) => _load(reader, values, count);
}
