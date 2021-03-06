using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using CatDb.General.Comparers;
using CatDb.General.Compression;
using CatDb.General.Extensions;
using CatDb.General.Persist;

namespace CatDb.Data
{
    public class Persist<T> : IPersist<T>
    {
        private readonly Action<BinaryWriter, T> _write;
        private readonly Func<BinaryReader, T> _read;

        private readonly Type _type;
        private readonly Func<Type, MemberInfo, int> _membersOrder;
        private readonly AllowNull _allowNull;

        public Persist(Func<Type, MemberInfo, int> membersOrder = null, AllowNull allowNull = AllowNull.None)
        {
            _type = typeof(T);
            _membersOrder = membersOrder;
            _allowNull = allowNull;

            _write = CreateWriteMethod().Compile();
            _read = CreateReadMethod().Compile();
        }

        public Expression<Action<BinaryWriter, T>> CreateWriteMethod()
        {
            var writer = Expression.Parameter(typeof(BinaryWriter));
            var item = Expression.Parameter(_type);

            return Expression.Lambda<Action<BinaryWriter, T>>(PersistHelper.CreateWriteBody(item, writer, _membersOrder, _allowNull), writer, item);
        }

        public Expression<Func<BinaryReader, T>> CreateReadMethod()
        {
            var reader = Expression.Parameter(typeof(BinaryReader), "reader");

            return Expression.Lambda<Func<BinaryReader, T>>(PersistHelper.CreateReadBody(reader, _type, _membersOrder, _allowNull), reader);
        }

        public void Write(BinaryWriter writer, T item)
        {
            _write(writer, item);
        }

        public T Read(BinaryReader reader)
        {
            return _read(reader);
        }
    }

    public class Persist : IPersist<object>
    {
        private readonly Action<BinaryWriter, object> _write;
        private readonly Func<BinaryReader, object> _read;

        private readonly Type _type;
        private readonly Func<Type, MemberInfo, int> _membersOrder;
        private readonly AllowNull _allowNull;

        public Persist(Type type, Func<Type, MemberInfo, int> membersOrder = null, AllowNull allowNull = AllowNull.None)
        {
            _type = type;
            _membersOrder = membersOrder;
            _allowNull = allowNull;

            _write = CreateWriteMethod().Compile();
            _read = CreateReadMethod().Compile();
        }

        public Expression<Action<BinaryWriter, object>> CreateWriteMethod()
        {
            var writer = Expression.Parameter(typeof(BinaryWriter));
            var item = Expression.Parameter(typeof(object));

            return Expression.Lambda<Action<BinaryWriter, object>>(PersistHelper.CreateWriteBody(Expression.Convert(item, _type), writer, _membersOrder, _allowNull), writer, item);
        }

        public Expression<Func<BinaryReader, object>> CreateReadMethod()
        {
            var reader = Expression.Parameter(typeof(BinaryReader), "reader");
            var body = PersistHelper.CreateReadBody(reader, _type, _membersOrder, _allowNull);

            return Expression.Lambda<Func<BinaryReader, object>>(Expression.Convert(body, typeof(object)), reader);
        }

        #region IPersist<object> Members

        public void Write(BinaryWriter writer, object item)
        {
            _write(writer, item);
        }

        public object Read(BinaryReader reader)
        {
            return _read(reader);
        }

        #endregion
    }

    public enum AllowNull : byte
    {
        /// <summary>
        /// Instance and all instance members and their members can be null.
        /// </summary>
        All,

        /// <summary>
        /// Instance can not be null, but instance members and their members can be null.
        /// </summary>
        OnlyMembers,

        /// <summary>
        /// Instance and instance members and their members cannot be null (the default and most space efficient variant).
        /// </summary>
        None
    }

    #region Examples

    //public class Tick
    //{
    //    public string Symbol { get; set; }
    //    public DateTime Timestamp { get; set; }
    //    public double Bid { get; set; }
    //    public double Ask { get; set; }
    //    public long Volume { get; set; }
    //    public string Provider { get; set; }
    //}

    //public class PersistTick : IPersist<Tick>
    //{
    //    public void Write(BinaryWriter writer, Tick item)
    //    {
    //        if (item.Symbol != null)
    //        {
    //            writer.Write(true);
    //            writer.Write(item.Symbol);
    //        }
    //        else
    //            writer.Write(false);

    //        writer.Write(item.Timestamp.Ticks);
    //        writer.Write(item.Bid);
    //        writer.Write(item.Ask);
    //        writer.Write(item.Volume);

    //        if (item.Provider != null)
    //        {
    //            writer.Write(true);
    //            writer.Write(item.Provider);
    //        }
    //        else
    //            writer.Write(false);
    //    }

    //    public Tick Read(BinaryReader reader)
    //    {
    //        var var1 = new Tick();

    //        var1.Symbol = reader.ReadBoolean() ? reader.ReadString() : null;
    //        var1.Timestamp = new DateTime(reader.ReadInt64());
    //        var1.Bid = reader.ReadDouble();
    //        var1.Ask = reader.ReadDouble();
    //        var1.Symbol = reader.ReadBoolean() ? reader.ReadString() : null;

    //        return var1;
    //    }
    //}

    #endregion

    public static class PersistHelper
    {
        public static Expression CreateWriteBody(Expression item, Expression writer, Func<Type, MemberInfo, int> membersOrder, AllowNull allowNull)
        {
            var list = new List<Expression>();

            if (DataType.IsPrimitiveType(item.Type) || item.Type.IsEnum || item.Type == typeof(Guid) || item.Type.IsKeyValuePair() || item.Type.IsArray || item.Type.IsList() || item.Type.IsDictionary() || item.Type.IsNullable())
                list.Add(BuildWrite(item, writer, membersOrder, allowNull, true));
            else
            {
                if (allowNull == AllowNull.All && !item.Type.IsStruct())
                    list.Add(Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(true)));

                foreach (var member in DataTypeUtils.GetPublicMembers(item.Type, membersOrder))
                    list.Add(BuildWrite(Expression.PropertyOrField(item, member.Name), writer, membersOrder, allowNull, false));

                if (allowNull == AllowNull.All && !item.Type.IsStruct())
                    return Expression.IfThenElse(Expression.NotEqual(item, Expression.Constant(null, item.Type)),
                            Expression.Block(list),
                            Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(false)));
            }

            return Expression.Block(list);
        }

        private static Expression BuildWrite(Expression item, Expression writer, Func<Type, MemberInfo, int> membersOrder, AllowNull allowNull, bool isTop)
        {
            var type = item.Type;
            var canBeNull = allowNull == AllowNull.All || (allowNull == AllowNull.OnlyMembers && !isTop);

            if (type == typeof(Guid))
                return GetWriteCommand(writer, Expression.Call(item, type.GetMethod("ToByteArray")), false);

            if (type.IsEnum)
                return GetWriteCommand(writer, Expression.Convert(item, item.Type.GetEnumUnderlyingType()), canBeNull);

            if (DataType.IsPrimitiveType(type))
                return GetWriteCommand(writer, item, canBeNull);

            if (type.IsKeyValuePair())
            {
                return Expression.Block(
                    BuildWrite(Expression.PropertyOrField(item, "Key"), writer, membersOrder, allowNull, false),
                    BuildWrite(Expression.PropertyOrField(item, "Value"), writer, membersOrder, allowNull, false)
                 );
            }

            if (type.IsArray || type.IsList())
            {
                if (!canBeNull)
                    return Expression.Block(Expression.Call(typeof(CountCompression).GetMethod("Serialize"), writer, Expression.Convert(type.IsArray ? Expression.ArrayLength(item) : Expression.Property(item, "Count"), typeof(ulong))),
                        item.For(i =>
                           WriteAssignedOrCurrentVariable(type.IsArray ? Expression.ArrayAccess(item, i) : item.This(i), writer, membersOrder, allowNull), 
                           Expression.Label()));

                return Expression.IfThenElse(Expression.NotEqual(item, Expression.Constant(null)),
                    Expression.Block(
                        Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(true)),
                        Expression.Call(typeof(CountCompression).GetMethod("Serialize"), writer, Expression.Convert(type.IsArray ? Expression.ArrayLength(item) : Expression.Property(item, "Count"), typeof(ulong))),
                        item.For(i =>
                        WriteAssignedOrCurrentVariable(type.IsArray ? Expression.ArrayAccess(item, i) : item.This(i), writer, membersOrder, allowNull),
                        Expression.Label())),
                    Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(false))
                    );
            }

            if (type.IsDictionary())
            {
                if (!DataType.IsPrimitiveType(type.GetGenericArguments()[0]) && !type.GetGenericArguments()[0].IsEnum && type != typeof(Guid))
                    throw new NotSupportedException($"Dictionarty<{type.GetGenericArguments()[0]}, TValue>");

                if (!canBeNull)
                    return Expression.Block(
                            Expression.Call(typeof(CountCompression).GetMethod("Serialize"), writer, Expression.Convert(Expression.Property(item, "Count"), typeof(ulong))),
                            item.ForEach(current =>
                            {
                                var kv = Expression.Variable(current.Type);

                                return Expression.Block(new[] { kv },
                                    Expression.Assign(kv, current),
                                    WriteAssignedOrCurrentVariable(Expression.PropertyOrField(kv, "Key"), writer, membersOrder, allowNull),
                                    WriteAssignedOrCurrentVariable(Expression.PropertyOrField(kv, "Value"), writer, membersOrder, allowNull)
                                );
                            }, Expression.Label())
                           );

                return Expression.IfThenElse(Expression.NotEqual(item, Expression.Constant(null)),
                    Expression.Block(
                        Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(true)),
                        Expression.Call(typeof(CountCompression).GetMethod("Serialize"), writer, Expression.Convert(Expression.Property(item, "Count"), typeof(ulong))),
                        item.ForEach(current =>
                        {
                            var kv = Expression.Variable(current.Type);

                            return Expression.Block(new[] { kv },
                                Expression.Assign(kv, current),
                                WriteAssignedOrCurrentVariable(Expression.PropertyOrField(kv, "Key"), writer, membersOrder, allowNull),
                                WriteAssignedOrCurrentVariable(Expression.PropertyOrField(kv, "Value"), writer, membersOrder, allowNull)
                            );
                        }, Expression.Label())),
                      Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(false))
                    );
            }

            if (type.IsNullable())
            {
                if (!canBeNull)
                    return BuildWrite(Expression.PropertyOrField(item, "Value"), writer, membersOrder, allowNull, false);

                return Expression.Block(Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.PropertyOrField(item, "HasValue")),
                        Expression.IfThen(Expression.PropertyOrField(item, "HasValue"), BuildWrite(Expression.PropertyOrField(item, "Value"), writer, membersOrder, allowNull, false)));
            }

            if (type.IsClass || type.IsStruct())
            {
                var variables = new List<ParameterExpression>();
                var list = new List<Expression>();

                if (canBeNull && !type.IsStruct())
                    list.Add(Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(true)));

                foreach (var member in DataTypeUtils.GetPublicMembers(type, membersOrder))
                {
                    if (DataType.IsPrimitiveType(type) || type.IsKeyValuePair())
                        list.Add(BuildWrite(Expression.PropertyOrField(item, member.Name), writer, membersOrder, allowNull, false));
                    else
                    {
                        var var = Expression.Variable(member.GetPropertyOrFieldType());
                        variables.Add(var);
                        list.Add(Expression.Assign(var, Expression.PropertyOrField(item, member.Name)));
                        list.Add(BuildWrite(var, writer, membersOrder, allowNull, false));
                    }
                }

                if (!canBeNull || type.IsStruct())
                    return Expression.Block(variables, list);

                return Expression.IfThenElse(Expression.NotEqual(item, Expression.Constant(null)),
                        Expression.Block(variables, list),
                        Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(false))
                    );
            }

            throw new NotSupportedException(item.Type.ToString());
        }

        private static Expression WriteAssignedOrCurrentVariable(Expression variable, Expression writer, Func<Type, MemberInfo, int> membersOrder, AllowNull allowNull)
        {
            var type = variable.Type;

            if (!DataType.IsPrimitiveType(type) && !type.IsEnum && type != typeof(Guid))
            {
                var var = Expression.Variable(type);
                return Expression.Block(new[] { var },
               Expression.Assign(var, variable),
               BuildWrite(var, writer, membersOrder, allowNull, false));
            }

            return BuildWrite(variable, writer, membersOrder, allowNull, false);
        }

        private static Expression GetWriteCommand(Expression writer, Expression item, bool canBeNull)
        {
            Debug.Assert(DataType.IsPrimitiveType(item.Type));

            var type = item.Type;

            if (type == typeof(Boolean) ||
                type == typeof(Char) ||
                type == typeof(SByte) ||
                type == typeof(Byte) ||
                type == typeof(Int16) ||
                type == typeof(Int32) ||
                type == typeof(UInt32) ||
                type == typeof(UInt16) ||
                type == typeof(Int64) ||
                type == typeof(UInt64) ||
                type == typeof(Single) ||
                type == typeof(Double) ||
                type == typeof(Decimal))
            {
                var writeAny = typeof(BinaryWriter).GetMethod("Write", new[] { type });
                return Expression.Call(writer, writeAny, item);

                //writer.Write(item);
            }

            if (type == typeof(DateTime) || type == typeof(TimeSpan))
            {
                var writeLong = typeof(BinaryWriter).GetMethod("Write", new[] { typeof(long) });
                return Expression.Call(writer, writeLong, Expression.PropertyOrField(item, "Ticks"));

                //writer.Write(item.Ticks);
            }

            if (type == typeof(String))
            {
                var writeBool = typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) });
                var writeString = typeof(BinaryWriter).GetMethod("Write", new[] { typeof(string) });

                if (!canBeNull)
                    return Expression.Call(writer, writeString, item);

                return Expression.IfThenElse(Expression.NotEqual(item, Expression.Constant(null)),
                    Expression.Block(Expression.Call(writer, writeBool, Expression.Constant(true)), Expression.Call(writer, writeString, item)),
                    Expression.Call(writer, writeBool, Expression.Constant(false))
                );

                //if (item != null)
                //{
                //    writer.Write(true);
                //    writer.Write(item);
                //}
                //else
                //    writer.Write(false);
            }

            if (type == typeof(byte[]))
            {
                var writeByteArray = typeof(BinaryWriter).GetMethod("Write", new[] { typeof(byte[]) });

                if (!canBeNull)
                    return Expression.Block(
                        Expression.Call(typeof(CountCompression).GetMethod("Serialize"), writer, Expression.ConvertChecked(Expression.Property(item, "Length"), typeof(ulong))),
                        Expression.Call(writer, writeByteArray, item)
                    );

                return Expression.IfThenElse(Expression.NotEqual(item, Expression.Constant(null)),
                    Expression.Block(
                        Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(true)),
                        Expression.Call(typeof(CountCompression).GetMethod("Serialize"), writer, Expression.ConvertChecked(Expression.Property(item, "Length"), typeof(ulong))),
                        Expression.Call(writer, writeByteArray, item)
                    ),
                    Expression.Call(writer, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) }), Expression.Constant(false))
                );

                //if (buffer != null)
                //{
                //    writer.Write(true);
                //    CountCompression.Serialize(writer, checked((long)buffer.Length));
                //    writer.Write(buffer);
                //}
                //else
                //    writer.Write(false);
            }

            throw new NotSupportedException(type.ToString());
        }

        public static Expression CreateReadBody(Expression reader, Type itemType, Func<Type, MemberInfo, int> membersOrder, AllowNull allowNull)
        {
            var item = Expression.Variable(itemType);

            var list = new List<Expression>();

            if (DataType.IsPrimitiveType(itemType) || itemType.IsEnum || itemType == typeof(Guid) || itemType.IsKeyValuePair() || itemType.IsArray || itemType.IsList() || itemType.IsDictionary() || itemType.IsNullable())
                return BuildRead(reader, itemType, membersOrder, allowNull, true);
            list.Add(Expression.Assign(item, Expression.New(itemType)));

            foreach (var member in DataTypeUtils.GetPublicMembers(itemType, membersOrder))
                list.Add(Expression.Assign(Expression.PropertyOrField(item, member.Name), BuildRead(reader, member.GetPropertyOrFieldType(), membersOrder, allowNull, false)));

            list.Add(Expression.Label(Expression.Label(itemType), item));

            if (allowNull == AllowNull.All && !itemType.IsStruct())
                return Expression.Condition(Expression.Call(reader, typeof(BinaryReader).GetMethod("ReadBoolean")),
                    Expression.Block(itemType, new[] { item }, list), Expression.Label(Expression.Label(itemType),
                        Expression.Constant(null, item.Type)));

            return Expression.Block(itemType, new[] { item }, list);
        }

        private static Expression BuildRead(Expression reader, Type itemType, Func<Type, MemberInfo, int> membersOrder, AllowNull allowNull, bool isTop)
        {
            var canBeNull = allowNull == AllowNull.All || (allowNull == AllowNull.OnlyMembers && !isTop);

            if (itemType == typeof(Guid))
                return Expression.New(itemType.GetConstructor(new[] { typeof(byte[]) }), GetReadCommand(reader, typeof(byte[]), false));

            if (itemType.IsEnum)
                return Expression.Convert(GetReadCommand(reader, itemType.GetEnumUnderlyingType(), canBeNull), itemType);

            if (DataType.IsPrimitiveType(itemType))
                return GetReadCommand(reader, itemType, canBeNull);

            if (itemType.IsKeyValuePair())
            {
                return Expression.New(
                        itemType.GetConstructor(new[] { itemType.GetGenericArguments()[0], itemType.GetGenericArguments()[1] }),
                        BuildRead(reader, itemType.GetGenericArguments()[0], membersOrder, allowNull, false), BuildRead(reader, itemType.GetGenericArguments()[1], membersOrder, allowNull, false)
                    );
            }

            if (itemType.IsArray || itemType.IsList() || itemType.IsDictionary())
            {
                var field = Expression.Variable(itemType);
                var lenght = Expression.Variable(typeof(int));

                var block = Expression.Block(
                    Expression.Assign(lenght, Expression.Convert(Expression.Call(typeof(CountCompression).GetMethod("Deserialize"), reader), typeof(int))),
                    itemType.IsDictionary() && itemType.GetGenericArguments()[0] == typeof(byte[]) ?
                        Expression.Assign(field, Expression.New(field.Type.GetConstructor(new[] { typeof(int), typeof(IEqualityComparer<byte[]>) }), lenght, Expression.Field(null, typeof(BigEndianByteArrayEqualityComparer), "Instance"))) :
                        Expression.Assign(field, Expression.New(field.Type.GetConstructor(new[] { typeof(int) }), lenght)),
                    field.For(i =>
                        {
                            if (itemType.IsArray)
                                return Expression.Assign(Expression.ArrayAccess(field, i), BuildRead(reader, itemType.GetElementType(), membersOrder, allowNull, false));
                            if (itemType.IsList())
                                return Expression.Call(field, field.Type.GetMethod("Add"), BuildRead(reader, itemType.GetGenericArguments()[0], membersOrder, allowNull, false));
                            return Expression.Call(field, field.Type.GetMethod("Add"),
                                BuildRead(reader, itemType.GetGenericArguments()[0], membersOrder, allowNull, false),
                                BuildRead(reader, itemType.GetGenericArguments()[1], membersOrder, allowNull, false)
                            );
                        },
                        Expression.Label(), lenght)
                    );

                if (canBeNull)
                    return Expression.Block(field.Type, new[] { field, lenght },
                        Expression.IfThenElse(Expression.Call(reader, typeof(BinaryReader).GetMethod("ReadBoolean")),
                            block,
                            Expression.Assign(field, Expression.Constant(null, field.Type))),
                        Expression.Label(Expression.Label(field.Type), field));

                return Expression.Block(field.Type, new[] { field, lenght },
                        block,
                        Expression.Label(Expression.Label(field.Type), field));
            }

            if (itemType.IsNullable())
            {
                if (!canBeNull)
                    return Expression.New(itemType.GetConstructor(new[] { itemType.GetGenericArguments()[0] }), BuildRead(reader, itemType.GetGenericArguments()[0], membersOrder, allowNull, false));

                return Expression.Condition(Expression.Call(reader, typeof(BinaryReader).GetMethod("ReadBoolean")),
                        Expression.New(itemType.GetConstructor(new[] { itemType.GetGenericArguments()[0] }), BuildRead(reader, itemType.GetGenericArguments()[0], membersOrder, allowNull, false)),
                        Expression.Constant(null, itemType));
            }

            if (itemType.IsClass || itemType.IsStruct())
            {
                var item = Expression.Variable(itemType);

                var list = new List<Expression> { Expression.Assign(item, Expression.New(item.Type)) };

                foreach (var member in DataTypeUtils.GetPublicMembers(itemType, membersOrder))
                    list.Add(Expression.Assign(Expression.PropertyOrField(item, member.Name), BuildRead(reader, member.GetPropertyOrFieldType(), membersOrder, allowNull, false)));

                if (!canBeNull || itemType.IsStruct())
                {
                    list.Add(Expression.Label(Expression.Label(item.Type), item));
                    return Expression.Block(item.Type, new[] { item }, list);
                }

                return Expression.Block(itemType, new[] { item },
                    Expression.IfThenElse(Expression.Call(reader, typeof(BinaryReader).GetMethod("ReadBoolean")),
                        Expression.Block(list),
                        Expression.Assign(item, Expression.Constant(null, itemType))),
                    Expression.Label(Expression.Label(item.Type), item));
            }

            throw new ArgumentException(itemType.ToString());
        }

        private static Expression GetReadCommand(Expression reader, Type itemType, bool canBeNull)
        {
            Debug.Assert(DataType.IsPrimitiveType(itemType));

            if (itemType == typeof(Boolean) || itemType == typeof(Char) || itemType == typeof(SByte) || itemType == typeof(Byte) ||
                    itemType == typeof(Int16) || itemType == typeof(UInt16) || itemType == typeof(Int32) || itemType == typeof(UInt32) || itemType == typeof(Int64) || itemType == typeof(UInt64) ||
                    itemType == typeof(Single) || itemType == typeof(Double) || itemType == typeof(Decimal))
            {
                var readAny = typeof(BinaryReader).GetMethod("Read" + itemType.Name);

                //return reader.ReadInt32();

                return Expression.Call(reader, readAny);
            }

            if (itemType == typeof(DateTime))
            {
                var readLong = typeof(BinaryReader).GetMethod("Read" + typeof(long).Name);
                return Expression.New(typeof(DateTime).GetConstructor(new[] { typeof(long) }), Expression.Call(reader, readLong));

                //return new DateTime(reader.ReadInt64());
            }

            if (itemType == typeof(TimeSpan))
            {
                var readLong = typeof(BinaryReader).GetMethod("Read" + typeof(long).Name);
                return Expression.New(typeof(TimeSpan).GetConstructor(new[] { typeof(long) }), Expression.Call(reader, readLong));

                //return new DateTime(reader.ReadInt64());
            }

            if (itemType == typeof(string))
            {
                var readBool = typeof(BinaryReader).GetMethod("Read" + typeof(bool).Name);
                var readString = typeof(BinaryReader).GetMethod("Read" + typeof(string).Name);

                if (!canBeNull)
                    return Expression.Call(reader, readString); //return reader.ReadString();

                //return reader.ReadBoolean() ? reader.ReadString() : null;

                return Expression.Condition(Expression.Call(reader, readBool), Expression.Call(reader, readString), Expression.Constant(null, typeof(string)));
            }

            if (itemType == typeof(byte[]))
            {
                var readBool = typeof(BinaryReader).GetMethod("Read" + typeof(bool).Name);
                var readBytes = typeof(BinaryReader).GetMethod("ReadBytes");

                var call = Expression.Call(typeof(CountCompression).GetMethod("Deserialize"), reader);

                if (!canBeNull)
                    return Expression.Call(reader, readBytes, Expression.Convert(call, typeof(int))); //return reader.ReadBytes((int)CountCompression.Deserialize(reader));

                //return reader.ReadBoolean() ? reader.ReadBytes((int)CountCompression.Deserialize(reader)) : null;

                return Expression.Condition(Expression.Call(reader, readBool), Expression.Call(reader, readBytes, Expression.Convert(call, typeof(int))), Expression.Constant(null, typeof(byte[])));
            }

            throw new NotSupportedException(itemType.ToString());
        }
    }
}
