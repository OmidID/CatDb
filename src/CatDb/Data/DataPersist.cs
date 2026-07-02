// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using System.Reflection;
using CatDb.General.Persist;

namespace CatDb.Data;

// Serializes/deserializes IData (= object) values.
// Write: cast object → T, serialize T fields.
// Read: deserialize T, box to object — no Data<T> wrapper heap allocation.
public class DataPersist : IPersist<IData>
{
    private readonly Action<BinaryWriter, IData> _write;
    private readonly Func<BinaryReader, IData> _read;

    private readonly Type _type;
    private readonly Func<Type, MemberInfo, int>? _membersOrder;
    private readonly AllowNull _allowNull;

    public DataPersist(Type type, Func<Type, MemberInfo, int>? membersOrder = null, AllowNull allowNull = AllowNull.None)
    {
        _type = type;
        _membersOrder = membersOrder;
        _allowNull = allowNull;

        _write = CreateWriteMethod().Compile();
        _read  = CreateReadMethod().Compile();
    }

    public void Write(BinaryWriter writer, IData item) => _write(writer, item);
    public IData Read(BinaryReader reader)             => _read(reader);

    public Expression<Action<BinaryWriter, IData>> CreateWriteMethod()
    {
        var writer    = Expression.Parameter(typeof(BinaryWriter), "writer");
        var idata     = Expression.Parameter(typeof(object), "idata");
        var dataValue = Expression.Variable(_type, "dataValue");

        // Cast object → T directly (unbox for value types, ref-cast for classes)
        var assign = Expression.Assign(dataValue, Expression.Convert(idata, _type));

        return Expression.Lambda<Action<BinaryWriter, IData>>(
            Expression.Block(new[] { dataValue }, assign,
                PersistHelper.CreateWriteBody(dataValue, writer, _membersOrder, _allowNull)),
            writer, idata);
    }

    public Expression<Func<BinaryReader, IData>> CreateReadMethod()
    {
        var reader   = Expression.Parameter(typeof(BinaryReader), "reader");
        var readBody = PersistHelper.CreateReadBody(reader, _type, _membersOrder, _allowNull);

        // Box/cast the deserialized T to object — no Data<T> wrapper
        return Expression.Lambda<Func<BinaryReader, IData>>(
            Expression.Convert(readBody, typeof(object)),
            reader);
    }
}
