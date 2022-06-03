using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Concurrent;
using System.Reflection;
using System.Reflection.Emit;
using Microsoft.CSharp;
using Environment = CatDb.General.Environment;

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

            if (Environment.RunningOnMono)
                return BuildTypeCodeDom(baseInterface, className, fieldsPrefix, types);
            return BuildTypeEmit(baseInterface, className, fieldsPrefix, types);
        }

        private static Type BuildTypeEmit(Type baseInterface, string className, string fieldsPrefix, params Type[] types)
        {
            var assemblyName = new AssemblyName(className);
            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.RunAndCollect);
            //AssemblyBuilder assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.RunAndCollect);
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

        private static Type BuildTypeCodeDom(Type baseInterface, string className, string fieldsPrefix, params Type[] types)
        {
            var compileUnit = new CodeCompileUnit();
            var globalNamespace = new CodeNamespace();

            globalNamespace.Imports.Add(new CodeNamespaceImport("System"));
            globalNamespace.Imports.Add(new CodeNamespaceImport("System.Collections.Generic"));
            globalNamespace.Imports.Add(new CodeNamespaceImport("System.Linq"));
            globalNamespace.Imports.Add(new CodeNamespaceImport("System.Text"));

            var classNamespace = new CodeNamespace("CatDb.Data");

            var generatedClass = new CodeTypeDeclaration(className)
            {
                IsClass = true,
                Attributes = MemberAttributes.Public
            };

            for (var i = 0; i < types.Length; i++)
                generatedClass.TypeParameters.Add(new CodeTypeParameter("T" + fieldsPrefix + i));

            if(baseInterface != null)
                generatedClass.BaseTypes.Add(baseInterface);

            var serializableAttribute = new CodeTypeReference(typeof(SerializableAttribute));
            generatedClass.CustomAttributes.Add(new CodeAttributeDeclaration(serializableAttribute));

            classNamespace.Types.Add(generatedClass);

            compileUnit.Namespaces.Add(globalNamespace);
            compileUnit.Namespaces.Add(classNamespace);

            var fields = new CodeMemberField[types.Length];

            for (var i = 0; i < fields.Length; i++)
            {
                fields[i] = new CodeMemberField("T" + fieldsPrefix + i, fieldsPrefix + i)
                {
                    Attributes = MemberAttributes.Public
                };
                generatedClass.Members.Add(fields[i]);
            }

            var defaultConstructor = new CodeConstructor
            {
                Attributes = MemberAttributes.Public
            };

            generatedClass.Members.Add(defaultConstructor);

            var constructor = new CodeConstructor
            {
                Attributes = MemberAttributes.Public
            };

            for (var i = 0; i < types.Length; i++)
            {
                var type = new CodeTypeReference("T" + fieldsPrefix + i);
                constructor.Parameters.Add(new CodeParameterDeclarationExpression(type, fieldsPrefix.ToLower() + i));
            }

            for (var i = 0; i < types.Length; i++)
            {
                var left = new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), fieldsPrefix + i);
                constructor.Statements.Add(new CodeAssignStatement(left, new CodeArgumentReferenceExpression(fieldsPrefix.ToLower() + i)));
            }

            generatedClass.Members.Add(constructor);

            var myAssemblyName = Assembly.GetExecutingAssembly().Location;
            string[] assemblies = { "System.dll", "mscorlib.dll", myAssemblyName };

            var parameters = new CompilerParameters(assemblies);

            CodeDomProvider runTimeProvider = new CSharpCodeProvider();
            parameters = new CompilerParameters(assemblies)
            {
                GenerateExecutable = false,
                GenerateInMemory = true,
                IncludeDebugInformation = true,
                CompilerOptions = "/optimize"
            };

            var compilerResults = runTimeProvider.CompileAssemblyFromDom(parameters, compileUnit);
            var generatedType = compilerResults.CompiledAssembly.GetTypes()[0];

            return generatedType.MakeGenericType(types);
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
