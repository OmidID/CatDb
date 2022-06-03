using System.Reflection;
using CatDb.General.Comparers;
using CatDb.General.Extensions;

namespace CatDb.Data
{
    public class CompareOption : IEquatable<CompareOption>
    {
        public readonly SortOrder SortOrder;
        public readonly ByteOrder ByteOrder;
        public readonly bool IgnoreCase;

        private CompareOption(SortOrder sortOrder, ByteOrder byteOrder, bool ignoreCase)
        {
            SortOrder = sortOrder;
            ByteOrder = byteOrder;
            IgnoreCase = ignoreCase;
        }

        public CompareOption(SortOrder sortOrder)
            : this(sortOrder, ByteOrder.Unspecified, false)
        {
        }

        public CompareOption(SortOrder sortOrder, ByteOrder byteOrder)
            : this(sortOrder, byteOrder, false)
        {
        }

        public CompareOption(ByteOrder byteOrder)
            : this(SortOrder.Ascending, byteOrder)
        {
        }

        public CompareOption(SortOrder sortOrder, bool ignoreCase)
            : this(sortOrder, ByteOrder.Unspecified, ignoreCase)
        {
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write((byte)SortOrder);
            writer.Write((byte)ByteOrder);
            writer.Write(IgnoreCase);
        }

        public static CompareOption Deserialize(BinaryReader reader)
        {
            var sortOrder = (SortOrder)reader.ReadByte();
            var byteOrder = (ByteOrder)reader.ReadByte();
            var ignoreCase = reader.ReadBoolean();

            return new CompareOption(sortOrder, byteOrder, ignoreCase);
        }

        public bool Equals(CompareOption other)
        {
            return SortOrder == other.SortOrder && ByteOrder == other.ByteOrder && IgnoreCase == other.IgnoreCase;
        }

        #region Utils

        public static CompareOption GetDefaultCompareOption(Type type)
        {
            if (type == typeof(byte[]))
                return new CompareOption(SortOrder.Ascending, ByteOrder.BigEndian);

            if (type == typeof(String))
                return new CompareOption(SortOrder.Ascending, false);

            return new CompareOption(SortOrder.Ascending);
        }

        public static CompareOption[] GetDefaultCompareOptions(Type type, Func<Type, MemberInfo, int> memberOrder = null)
        {
            if (DataType.IsPrimitiveType(type))
                return new[] { GetDefaultCompareOption(type) };

            if (type == typeof(Guid))
                return new[] { GetDefaultCompareOption(type) };

            if (type.IsClass || type.IsStruct())
                return DataTypeUtils.GetPublicMembers(type, memberOrder).Select(x => GetDefaultCompareOption(x.GetPropertyOrFieldType())).ToArray();

            throw new NotSupportedException(type.ToString());
        }

        public static void CheckCompareOption(Type type, CompareOption option)
        {
            if (!DataType.IsPrimitiveType(type) && type != typeof(Guid))
                throw new NotSupportedException($"The type '{type}' is not primitive.");

            if (type == typeof(string))
            {
                if (option.ByteOrder != ByteOrder.Unspecified)
                    throw new ArgumentException("String can't have ByteOrder option.");
            }
            else if (type == typeof(byte[]))
            {
                if (option.ByteOrder == ByteOrder.Unspecified)
                    throw new ArgumentException("byte[] must have ByteOrder option.");
            }
            else
            {
                if (option.ByteOrder != ByteOrder.Unspecified)
                    throw new ArgumentException($"{type} does not support ByteOrder option.");
            }
        }

        public static void CheckCompareOptions(Type type, CompareOption[] compareOptions, Func<Type, MemberInfo, int> memberOrder = null)
        {
            if (type.IsClass || type.IsStruct())
            {
                var i = 0;
                foreach (var member in DataTypeUtils.GetPublicMembers(type, memberOrder).Select(x => x.GetPropertyOrFieldType()).ToArray())
                    CheckCompareOption(member, compareOptions[i++]);
            }
            else
                CheckCompareOption(type, compareOptions[0]);
        }

        #endregion

        public static CompareOption Ascending => new(SortOrder.Ascending);

        public static CompareOption Descending => new(SortOrder.Descending);
    }
}
