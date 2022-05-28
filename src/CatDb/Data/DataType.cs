using System.Collections;

namespace CatDb.Data
{
    /*  examples:

            DataType type1 = DataType.Boolean;
            DataType type2 = DataType.Int32;
            DataType type3 = DataType.String;

            DataType type4 = DataType.Slotes(
                DataType.String,
                DataType.DateTime,
                DataType.Double,
                DataType.Double,
                DataType.Int64
                );

            DataType type5 = DataType.Array(DataType.String);
            DataType type6 = DataType.List(DataType.String);
            DataType type7 = DataType.Dictionary(DataType.Int32, DataType.String);

            DataType type8 = DataType.Slotes(
                DataType.String,
                DataType.DateTime,
                DataType.Double,
                DataType.Double,
                DataType.Slotes(
                    DataType.String,
                    DataType.String),
                    DataType.Array(DataType.Boolean),
                DataType.Int32,
                DataType.Dictionary(DataType.Int32, DataType.String),
                DataType.Boolean,
                DataType.List(DataType.Array(DataType.DateTime)),
                DataType.Dictionary(DataType.String, DataType.List(DataType.DateTime))
                );
    */

    public sealed class DataType : IEquatable<DataType>, IEnumerable<DataType>
    {
        private static readonly Type[] PrimitiveTypes;
        private static readonly Dictionary<Type, DataType> PrimitiveMap;

        public static readonly DataType Boolean = new(Code.Boolean, null);
        public static readonly DataType Char = new(Code.Char, null);
        public static readonly DataType SByte = new(Code.SByte, null);
        public static readonly DataType Byte = new(Code.Byte, null);
        public static readonly DataType Int16 = new(Code.Int16, null);
        public static readonly DataType UInt16 = new(Code.UInt16, null);
        public static readonly DataType Int32 = new(Code.Int32, null);
        public static readonly DataType UInt32 = new(Code.UInt32, null);
        public static readonly DataType Int64 = new(Code.Int64, null);
        public static readonly DataType UInt64 = new(Code.UInt64, null);
        public static readonly DataType Single = new(Code.Single, null);
        public static readonly DataType Double = new(Code.Double, null);
        public static readonly DataType Decimal = new(Code.Decimal, null);
        public static readonly DataType DateTime = new(Code.DateTime, null);
        public static readonly DataType TimeSpan = new(Code.TimeSpan, null);
        public static readonly DataType String = new(Code.String, null);
        public static readonly DataType ByteArray = new(Code.ByteArray, null);

        private int? _cachedHashCode;
        private string _cachedToString;
        private byte[] _cachedSerialize;

        private readonly Code _code;
        private readonly DataType[] _types;

        static DataType()
        {
            PrimitiveTypes = new Type[(int)Code.ByteArray + 1];

            PrimitiveTypes[(int)Code.Boolean] = typeof(Boolean);
            PrimitiveTypes[(int)Code.Char] = typeof(Char);
            PrimitiveTypes[(int)Code.SByte] = typeof(SByte);
            PrimitiveTypes[(int)Code.Byte] = typeof(Byte);
            PrimitiveTypes[(int)Code.Int16] = typeof(Int16);
            PrimitiveTypes[(int)Code.Int32] = typeof(Int32);
            PrimitiveTypes[(int)Code.UInt32] = typeof(UInt32);
            PrimitiveTypes[(int)Code.UInt16] = typeof(UInt16);
            PrimitiveTypes[(int)Code.Int64] = typeof(Int64);
            PrimitiveTypes[(int)Code.UInt64] = typeof(UInt64);
            PrimitiveTypes[(int)Code.Single] = typeof(Single);
            PrimitiveTypes[(int)Code.Double] = typeof(Double);
            PrimitiveTypes[(int)Code.Decimal] = typeof(Decimal);
            PrimitiveTypes[(int)Code.DateTime] = typeof(DateTime);
            PrimitiveTypes[(int)Code.TimeSpan] = typeof(TimeSpan);
            PrimitiveTypes[(int)Code.String] = typeof(String);
            PrimitiveTypes[(int)Code.ByteArray] = typeof(byte[]);

            PrimitiveMap = new Dictionary<Type, DataType>
            {
                [typeof(Boolean)] = Boolean,
                [typeof(Char)] = Char,
                [typeof(SByte)] = SByte,
                [typeof(Byte)] = Byte,
                [typeof(Int16)] = Int16,
                [typeof(Int32)] = Int32,
                [typeof(UInt32)] = UInt32,
                [typeof(UInt16)] = UInt16,
                [typeof(Int64)] = Int64,
                [typeof(UInt64)] = UInt64,
                [typeof(Single)] = Single,
                [typeof(Double)] = Double,
                [typeof(Decimal)] = Decimal,
                [typeof(DateTime)] = DateTime,
                [typeof(TimeSpan)] = TimeSpan,
                [typeof(String)] = String,
                [typeof(byte[])] = ByteArray
            };
        }

        private DataType(Code code, params DataType[] types)
        {
            this._code = code;
            this._types = types;
        }

        private int InternalGetHashCode()
        {
            if (IsPrimitive)
                return (int)_code;

            var hashcode = 37;
            hashcode = 17 * hashcode + (int)_code;

            for (var i = 0; i < _types.Length; i++)
                hashcode = 17 * hashcode + _types[i].InternalGetHashCode();

            return hashcode;
        }

        private string InternalToString()
        {
            if (IsPrimitive)
                return _code.ToString();

            var s = System.String.Join(", ", _types.Select(x => x.InternalToString()));

            if (IsSlots)
                return $"({s})";
            else
                return $"{_code}<{s}>";
        }

        private void InternalSerialize(BinaryWriter writer)
        {
            writer.Write((byte)_code);
            if (IsPrimitive)
                return;

            writer.Write(checked((byte)_types.Length));
            for (var i = 0; i < _types.Length; i++)
                _types[i].InternalSerialize(writer);
        }

        public bool IsPrimitive => _code < Code.Slots;

        public bool IsSlots => _code == Code.Slots;

        public bool IsArray => _code == Code.Array;

        public bool IsList => _code == Code.List;

        public bool IsDictionary => _code == Code.Dictionary;

        public bool AreAllTypesPrimitive
        {
            get
            {
                if (IsPrimitive)
                    throw new InvalidOperationException($"The type {this} is primitive");

                return _types.All(x => x.IsPrimitive);
            }
        }

        public DataType this[int index]
        {
            get
            {
                if (IsPrimitive)
                    throw new InvalidOperationException($"The type {this} is primitive");

                return _types[index];
            }
        }

        public int TypesCount
        {
            get
            {
                if (IsPrimitive)
                    throw new InvalidOperationException($"The type {this} is primitive");

                return _types.Length;
            }
        }

        public Type PrimitiveType
        {
            get
            {
                if (!IsPrimitive)
                    throw new InvalidOperationException($"The type {this} is not primitive");

                return PrimitiveTypes[(int)_code];
            }
        }

        public bool Equals(DataType other)
        {
            if (ReferenceEquals(this, other))
                return true;

            if (ReferenceEquals(null, other))
                return false;

            if (_code != other._code)
                return false;

            if (IsPrimitive)
                return true;

            var types1 = _types;
            var types2 = other._types;

            if (types1.Length != types2.Length)
                return false;

            for (var i = 0; i < types1.Length; i++)
            {
                if (!types1[i].Equals(types2[i]))
                    return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is DataType))
                return false;

            return Equals((DataType)obj);
        }

        public override int GetHashCode()
        {
            if (!_cachedHashCode.HasValue)
                _cachedHashCode = InternalGetHashCode();

            return _cachedHashCode.Value;
        }

        public override string ToString()
        {
            if (_cachedToString == null)
                _cachedToString = InternalToString();

            return _cachedToString;
        }

        public IEnumerator<DataType> GetEnumerator()
        {
            for (var i = 0; i < _types.Length; i++)
                yield return _types[i];
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Serialize(BinaryWriter writer)
        {
            if (_cachedSerialize == null)
            {
                using var ms = new MemoryStream();
                InternalSerialize(new BinaryWriter(ms));
                _cachedSerialize = ms.ToArray();
            }

            writer.Write(_cachedSerialize);
        }

        public static DataType Deserialize(BinaryReader reader)
        {
            var code = (Code)reader.ReadByte();
            if (code < Code.Slots)
                return new DataType(code, null);

            var types = new DataType[reader.ReadByte()];
            for (var i = 0; i < types.Length; i++)
                types[i] = Deserialize(reader);

            return new DataType(code, types);
        }

        public static DataType Slots(params DataType[] slots)
        {
            if (slots.Length == 0)
                throw new ArgumentException("slots.Length == 0");

            if (slots.Length > byte.MaxValue)
                throw new ArgumentException("slots.Length > 255");

            return new DataType(Code.Slots, slots);
        }

        public static DataType Array(DataType T)
        {
            return new DataType(Code.Array, T);
        }

        public static DataType List(DataType T)
        {
            return new DataType(Code.List, T);
        }

        public static DataType Dictionary(DataType key, DataType value)
        {
            return new DataType(Code.Dictionary, key, value);
        }

        public static DataType FromPrimitiveType(Type type)
        {
            return PrimitiveMap[type];
        }

        public static bool IsPrimitiveType(Type type)
        {
            return PrimitiveMap.ContainsKey(type);
        }

        public static bool operator ==(DataType type1, DataType type2)
        {
            if (ReferenceEquals(type1, type2))
                return true;

            if (ReferenceEquals(type1, null))
                return false;

            return type1.Equals(type2);
        }

        public static bool operator !=(DataType type1, DataType type2)
        {
            return !(type1 == type2);
        }

        private enum Code : byte
        {
            Boolean = TypeCode.Boolean,     // 3  
            Char = TypeCode.Char,           // 4
            SByte = TypeCode.SByte,         // 5
            Byte = TypeCode.Byte,           // 6
            Int16 = TypeCode.Int16,         // 7
            UInt16 = TypeCode.UInt16,       // 8
            Int32 = TypeCode.Int32,         // 9
            UInt32 = TypeCode.UInt32,       // 10
            Int64 = TypeCode.Int64,         // 11
            UInt64 = TypeCode.UInt64,       // 12
            Single = TypeCode.Single,       // 13
            Double = TypeCode.Double,       // 14
            Decimal = TypeCode.Decimal,     // 15
            DateTime = TypeCode.DateTime,   // 16
            TimeSpan = 17,
            String = TypeCode.String,       // 18
            ByteArray = 19,

            Slots = 20,
            Array = 21,
            List = 22,
            Dictionary = 23
        }
    }
}