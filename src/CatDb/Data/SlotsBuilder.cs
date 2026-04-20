using System.Collections.Concurrent;
using System.Reflection;
using System.Reflection.Emit;

namespace CatDb.Data
{
    public class SlotsBuilder
    {
        private static readonly ConcurrentDictionary<TypeArray, Type> Map = new();

        private static Type BuildType(Type baseInterface, string className, string fieldsPrefix, params Type[] types)
        {
            if (className == null)
                throw new ArgumentNullException("className");

            if (fieldsPrefix == null)
                throw new ArgumentNullException("fieldsPrefix");

            if (types.Length == 0)
                throw new ArgumentException("types.Length == 0");

            return BuildTypeEmit(baseInterface, className, fieldsPrefix, types);
        }

        private static Type BuildTypeEmit(Type baseInterface, string className, string fieldsPrefix, params Type[] types)
        {
            var assemblyName = new AssemblyName(className);
            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.RunAndCollect);
            var moduleBuilder = assemblyBuilder.DefineDynamicModule(assemblyName.Name);

            var genericParameters = new string[types.Length];
            for (var i = 0; i < types.Length; i++)
                genericParameters[i] = "T" + fieldsPrefix + i;

            var typeBuilder = moduleBuilder.DefineType(className, TypeAttributes.Class | TypeAttributes.Public);

            if(baseInterface != null)
                typeBuilder.AddInterfaceImplementation(baseInterface);

            var customAttribute = new CustomAttributeBuilder(typeof(SerializableAttribute).GetConstructor(Type.EmptyTypes), new object[] { });
            typeBuilder.SetCustomAttribute(customAttribute);

            var typeParams = typeBuilder.DefineGenericParameters(genericParameters);

            var fields = new FieldBuilder[types.Length];
            for (var i = 0; i < types.Length; i++)
                fields[i] = typeBuilder.DefineField(fieldsPrefix + i, typeParams[i], FieldAttributes.Public);

            typeBuilder.DefineDefaultConstructor(MethodAttributes.Public);

            var constr = typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, typeParams);
            var ilGenerator = constr.GetILGenerator();

            for (var i = 0; i < types.Length; i++)
            {
                ilGenerator.Emit(OpCodes.Ldarg_0);
                ilGenerator.Emit(OpCodes.Ldarg_S, i + 1);
                ilGenerator.Emit(OpCodes.Stfld, fields[i]);
            }

            ilGenerator.Emit(OpCodes.Ret);

            return typeBuilder.CreateType().MakeGenericType(types);
        }

        public static Type BuildType(params Type[] types)
        {
            if (types.Length == 0)
                throw new ArgumentException("types array is empty.");

            return types.Length switch
            {
                01 => typeof(Slots<>).MakeGenericType(types),
                02 => typeof(Slots<,>).MakeGenericType(types),
                03 => typeof(Slots<,,>).MakeGenericType(types),
                04 => typeof(Slots<,,,>).MakeGenericType(types),
                05 => typeof(Slots<,,,,>).MakeGenericType(types),
                06 => typeof(Slots<,,,,,>).MakeGenericType(types),
                07 => typeof(Slots<,,,,,,>).MakeGenericType(types),
                08 => typeof(Slots<,,,,,,,>).MakeGenericType(types),
                09 => typeof(Slots<,,,,,,,,>).MakeGenericType(types),
                10 => typeof(Slots<,,,,,,,,,>).MakeGenericType(types),
                11 => typeof(Slots<,,,,,,,,,,>).MakeGenericType(types),
                12 => typeof(Slots<,,,,,,,,,,,>).MakeGenericType(types),
                13 => typeof(Slots<,,,,,,,,,,,,>).MakeGenericType(types),
                14 => typeof(Slots<,,,,,,,,,,,,,>).MakeGenericType(types),
                15 => typeof(Slots<,,,,,,,,,,,,,,>).MakeGenericType(types),
                16 => typeof(Slots<,,,,,,,,,,,,,,,>).MakeGenericType(types),
                _ => Map.GetOrAdd(new TypeArray(types), BuildType(typeof(ISlots), "Slots", "Slot", types))
            };
        }

        private class TypeArray : IEquatable<TypeArray>
        {
            private int? _hashcode;

            private readonly Type[] _types;

            public TypeArray(Type[] types)
            {
                _types = types;
            }

            public bool Equals(TypeArray other)
            {
                if (ReferenceEquals(this, other))
                    return true;

                if (ReferenceEquals(other, null))
                    return false;

                if (_types.Length != other._types.Length)
                    return false;

                for (var i = 0; i < _types.Length; i++)
                {
                    if (_types[i] != other._types[i])
                        return false;
                }

                return true;
            }

            public override int GetHashCode()
            {
                if (_hashcode == null)
                {
                    var code = 0;
                    for (var i = 0; i < _types.Length; i++)
                        code ^= _types[i].GetHashCode();

                    _hashcode = code;
                }

                return _hashcode.Value;
            }
        }
    }
}
