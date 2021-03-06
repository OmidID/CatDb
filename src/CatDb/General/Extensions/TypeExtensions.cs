using System.Reflection;

namespace CatDb.General.Extensions
{
    public static class TypeExtensions
    {
        public static bool IsStruct(this Type type)
        {
            return type.IsValueType && !type.IsPrimitive && !type.IsEnum;
        }

        public static bool IsInheritInterface(this Type type, Type @interface)
        {
            if (!@interface.IsInterface)
                throw new ArgumentException($"The type '{@interface.Name}' has to be an interface.");

            return type.GetInterfaces().FirstOrDefault(x => x == @interface) != null;
        }

        public static IEnumerable<MemberInfo> GetPublicReadWritePropertiesAndFields(this Type type)
        {
            foreach (var member in type.GetMembers(BindingFlags.Public | BindingFlags.Instance))
            {
                if (member.MemberType == MemberTypes.Field)
                {
                    var field = (FieldInfo)member;
                    if (field.IsInitOnly)
                        continue;

                    yield return member;
                }

                if (member.MemberType == MemberTypes.Property)
                {
                    var property = (PropertyInfo)member;
                    if (property.GetAccessors(false).Length != 2)
                        continue;

                    yield return member;
                }
            }
        }

        public static Type GetPropertyOrFieldType(this MemberInfo member)
        {
            return member.MemberType switch
            {
                MemberTypes.Property => ((PropertyInfo)member).PropertyType,
                MemberTypes.Field => ((FieldInfo)member).FieldType,
                _ => throw new NotSupportedException(member.MemberType.ToString())
            };
        }

        public static bool HasDefaultConstructor(this Type type)
        {
            return type.GetConstructor(new Type[] { }) != null;
        }

        public static bool IsDictionary(this Type type)
        {
            return type.Name == typeof(Dictionary<,>).Name;
        }

        public static bool IsList(this Type type)
        {
            return type.Name == typeof(List<>).Name;
        }

        public static bool IsKeyValuePair(this Type type)
        {
            return type.Name == typeof(KeyValuePair<,>).Name;
        }

        public static bool IsNullable(this Type type)
        {
            return type.Name == typeof(Nullable<>).Name;
        }
    }
}
