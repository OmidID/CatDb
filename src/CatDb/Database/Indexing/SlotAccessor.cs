// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using System.Reflection;
using CatDb.Data;

namespace CatDb.Database.Indexing;

/// <summary>
/// Compiles efficient delegates to extract slot values from IData (= object) records
/// and to build IData (= object) keys for index tables.
///
/// IData is now a global alias for System.Object — records/keys are stored as their
/// actual CLR types (no Data&lt;T&gt; wrapper). Expression trees cast object → T directly
/// and box T → object on output. This is called once per index at setup time, not per op.
/// </summary>
internal static class SlotAccessor
{
    /// <summary>
    /// Creates a delegate that extracts one or more slots from an object record
    /// and produces an object suitable as an index table key.
    /// Single slot → boxes the slot value. Composite → boxes Slots&lt;T0,T1,...&gt;.
    /// </summary>
    internal static Func<IData, IData> BuildExtractor(Type recordType, int[] slotIndices)
    {
        if (slotIndices.Length == 0)
            throw new ArgumentException("Must specify at least one slot index.");

        // input: object = actual record instance (cast to recordType)
        var inputParam  = Expression.Parameter(typeof(object), "record");
        var valueAccess = Expression.Convert(inputParam, recordType);

        if (slotIndices.Length == 1)
        {
            var slotMember = GetSlotMember(recordType, slotIndices[0]);
            // Normalize enum → underlying and Nullable<T> → T so the extracted value matches the
            // index table's normalized storage CLR type (see NormalizeStorageValue / NormalizeStorageType).
            var slotAccess = NormalizeStorageValue(AccessMember(valueAccess, slotMember));

            // box slot value to object
            var castResult = Expression.Convert(slotAccess, typeof(object));
            return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
        }
        else
        {
            var slotMembers  = slotIndices.Select(i => GetSlotMember(recordType, i)).ToArray();
            var slotAccesses = slotMembers.Select(m => NormalizeStorageValue(AccessMember(valueAccess, m))).ToArray();
            var slotTypes    = slotAccesses.Select(a => a.Type).ToArray();
            var slotsType    = SlotsBuilder.BuildType(slotTypes);

            var slotsCtor    = slotsType.GetConstructors()
                .First(c => c.GetParameters().Length == slotTypes.Length);
            var newSlots   = Expression.New(slotsCtor, slotAccesses);
            var castResult = Expression.Convert(newSlots, typeof(object));
            return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
        }
    }

    /// <summary>
    /// Builds composite key extractor: appends primary key to indexed field values.
    /// Input: (object record, object primaryKey) → object Slots&lt;fields…, pk&gt;
    /// </summary>
    internal static Func<IData, IData, IData> BuildNonUniqueKeyBuilder(
        Type recordType, int[] slotIndices, Type primaryKeyType)
    {
        var recordParam = Expression.Parameter(typeof(object), "record");
        var keyParam    = Expression.Parameter(typeof(object), "key");

        // cast object → recordType / primaryKeyType, then normalize enum/Nullable to storage form.
        var recordValue = Expression.Convert(recordParam, recordType);
        var keyValue    = NormalizeStorageValue(Expression.Convert(keyParam, primaryKeyType));

        Expression[] slotAccesses;
        if (DataType.IsPrimitiveType(recordType))
        {
            slotAccesses = [NormalizeStorageValue(recordValue)];
        }
        else
        {
            var slotMembers = slotIndices.Select(i => GetSlotMember(recordType, i)).ToArray();
            slotAccesses    = slotMembers.Select(m => NormalizeStorageValue(AccessMember(recordValue, m))).ToArray();
        }

        var slotTypes = slotAccesses.Select(a => a.Type).ToArray();
        var allTypes  = new Type[slotTypes.Length + 1];
        Array.Copy(slotTypes, allTypes, slotTypes.Length);
        allTypes[^1] = keyValue.Type; // normalized primary-key storage type

        var compositeType  = SlotsBuilder.BuildType(allTypes);
        var compositeCtor  = compositeType.GetConstructors()
            .First(c => c.GetParameters().Length == allTypes.Length);

        var allAccesses = new Expression[slotAccesses.Length + 1];
        Array.Copy(slotAccesses, allAccesses, slotAccesses.Length);
        allAccesses[^1] = keyValue;

        var newComposite = Expression.New(compositeCtor, allAccesses);
        var castResult   = Expression.Convert(newComposite, typeof(object));

        return Expression.Lambda<Func<IData, IData, IData>>(castResult, recordParam, keyParam).Compile();
    }

    /// <summary>
    /// Extracts the field portion (strips primary key suffix) from a composite key object.
    /// Single field → boxes field value. Multi-field → boxes sub-Slots.
    /// </summary>
    internal static Func<IData, IData> BuildFieldExtractorFromCompositeKey(
        Type compositeKeyType, int fieldCount)
    {
        var inputParam  = Expression.Parameter(typeof(object), "compositeKey");
        var valueAccess = Expression.Convert(inputParam, compositeKeyType);

        if (fieldCount == 1)
        {
            var slotMember = GetSlotMember(compositeKeyType, 0);
            var slotAccess = AccessMember(valueAccess, slotMember);
            var castResult = Expression.Convert(slotAccess, typeof(object));
            return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
        }
        else
        {
            var members    = Enumerable.Range(0, fieldCount)
                .Select(i => GetSlotMember(compositeKeyType, i)).ToArray();
            var types      = members.Select(GetMemberType).ToArray();
            var slotsType  = SlotsBuilder.BuildType(types);

            var accesses   = members.Select(m => AccessMember(valueAccess, m)).ToArray();
            var slotsCtor  = slotsType.GetConstructors()
                .First(c => c.GetParameters().Length == types.Length);
            var newSlots   = Expression.New(slotsCtor, accesses);
            var castResult = Expression.Convert(newSlots, typeof(object));
            return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
        }
    }

    /// <summary>Extracts the primary key (last slot) from a non-unique composite index key.</summary>
    internal static Func<IData, IData> BuildPrimaryKeyExtractorFromCompositeKey(
        Type compositeKeyType, int totalSlotCount)
    {
        var inputParam  = Expression.Parameter(typeof(object), "compositeKey");
        var valueAccess = Expression.Convert(inputParam, compositeKeyType);

        var pkMember   = GetSlotMember(compositeKeyType, totalSlotCount - 1);
        var pkAccess   = AccessMember(valueAccess, pkMember);
        var castResult = Expression.Convert(pkAccess, typeof(object));
        return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
    }

    /// <summary>
    /// Builds a "from" composite scan key: given a field value object, appends default(PK).
    /// </summary>
    internal static Func<IData, IData> BuildScanFromKeyBuilder(
        Type compositeKeyType, Type fieldType, Type primaryKeyType, int fieldCount)
    {
        var inputParam = Expression.Parameter(typeof(object), "fieldValue");
        var totalSlots = fieldCount + 1;
        var ctorArgs   = new Expression[totalSlots];

        // Cast input object → field type, then extract each needed slot
        var castInput = Expression.Convert(inputParam, fieldType);
        if (fieldCount == 1)
        {
            ctorArgs[0] = castInput;
        }
        else
        {
            for (int i = 0; i < fieldCount; i++)
            {
                var slotMember = GetSlotMember(fieldType, i);
                ctorArgs[i] = AccessMember(castInput, slotMember);
            }
        }

        ctorArgs[^1] = Expression.Default(primaryKeyType);

        var compositeCtor = compositeKeyType.GetConstructors()
            .First(c => c.GetParameters().Length == totalSlots);
        var newComposite = Expression.New(compositeCtor, ctorArgs);
        var castResult   = Expression.Convert(newComposite, typeof(object));

        return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
    }

    /// <summary>
    /// Compiles equality comparer for IData (= object) field values.
    /// For Slots types performs structural field-by-field comparison.
    /// </summary>
    internal static Func<IData, IData, bool> BuildFieldEqualityComparer(Type fieldType)
    {
        var aParam = Expression.Parameter(typeof(object), "a");
        var bParam = Expression.Parameter(typeof(object), "b");

        // Cast object → fieldType directly (no Data<T> intermediary)
        var valueA = Expression.Convert(aParam, fieldType);
        var valueB = Expression.Convert(bParam, fieldType);

        Expression equalsExpr;

        if (typeof(ISlots).IsAssignableFrom(fieldType))
        {
            var members  = DataTypeUtils.GetPublicMembers(fieldType).ToArray();
            Expression? combined = null;
            foreach (var member in members)
            {
                var memberType      = GetMemberType(member);
                var accessA         = AccessMember(valueA, member);
                var accessB         = AccessMember(valueB, member);
                var slotComparerType = typeof(System.Collections.Generic.EqualityComparer<>).MakeGenericType(memberType);
                var slotDefaultProp  = slotComparerType.GetProperty("Default", BindingFlags.Static | BindingFlags.Public)!;
                var slotEqualsMethod = slotComparerType.GetMethod("Equals", [memberType, memberType])!;
                var slotComparer     = Expression.Property(null, slotDefaultProp);
                var slotEquals       = Expression.Call(slotComparer, slotEqualsMethod, accessA, accessB);
                combined = combined == null ? slotEquals : Expression.AndAlso(combined, slotEquals);
            }
            equalsExpr = combined ?? Expression.Constant(true);
        }
        else
        {
            var comparerType  = typeof(System.Collections.Generic.EqualityComparer<>).MakeGenericType(fieldType);
            var defaultProp   = comparerType.GetProperty("Default", BindingFlags.Static | BindingFlags.Public)!;
            var equalsMethod  = comparerType.GetMethod("Equals", [fieldType, fieldType])!;
            var comparer      = Expression.Property(null, defaultProp);
            equalsExpr        = Expression.Call(comparer, equalsMethod, valueA, valueB);
        }

        return Expression.Lambda<Func<IData, IData, bool>>(equalsExpr, aParam, bParam).Compile();
    }

    internal static Type GetSlotType(Type compositeKeyType, int slotIndex)
        => GetMemberType(GetSlotMember(compositeKeyType, slotIndex));

    /// <summary>
    /// Builds a prefix seek key: given prefix value object, fills prefix slots and defaults the rest.
    /// </summary>
    internal static Func<IData, IData> BuildPrefixSeekKeyBuilder(Type compositeKeyType, Type prefixType, int prefixLen)
    {
        var inputParam = Expression.Parameter(typeof(object), "prefix");

        var ctor   = compositeKeyType.GetConstructors()
            .OrderByDescending(c => c.GetParameters().Length).First();
        var ps     = ctor.GetParameters();
        var args   = new Expression[ps.Length];

        // Cast object → prefixType
        var prefixValue = Expression.Convert(inputParam, prefixType);

        if (prefixLen == 1)
        {
            args[0] = prefixValue;
        }
        else
        {
            for (var i = 0; i < prefixLen; i++)
                args[i] = AccessMember(prefixValue, GetSlotMember(prefixType, i));
        }
        for (var i = prefixLen; i < ps.Length; i++)
            args[i] = Expression.Default(ps[i].ParameterType);

        var newComposite = Expression.New(ctor, args);
        var castResult   = Expression.Convert(newComposite, typeof(object));
        return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
    }

    // ── Storage-type normalization ───────────────────────────────────────────
    // The index table's key CLR type is rebuilt from its (normalized) DataType on reopen
    // (anonymous Slots types are regenerated), so index keys MUST use the same normalized
    // scalar types the persist/DataType layer uses: enum → underlying integral, Nullable<T> → T.
    // Raw enum/Nullable values would otherwise be boxed as their declared type and fail the
    // index table's cast (InvalidCastException for enum) or its comparer (NullReferenceException
    // for Nullable). Primitives and everything else pass through unchanged.

    /// <summary>Maps a member CLR type to its normalized index-storage type (enum → underlying, Nullable&lt;T&gt; → T).</summary>
    internal static Type NormalizeStorageType(Type t)
    {
        if (t.IsEnum)
            return t.GetEnumUnderlyingType();
        var underlying = Nullable.GetUnderlyingType(t);
        return underlying is not null ? NormalizeStorageType(underlying) : t;
    }

    /// <summary>Wraps a value expression so it yields the normalized storage value (see <see cref="NormalizeStorageType"/>).</summary>
    private static Expression NormalizeStorageValue(Expression access)
    {
        var t = access.Type;
        if (t.IsEnum)
            return Expression.Convert(access, t.GetEnumUnderlyingType());

        var underlying = Nullable.GetUnderlyingType(t);
        if (underlying is not null)
        {
            // null → default(underlying): keeps the value seekable and avoids the comparer NRE.
            Expression value = Expression.Call(access, t.GetMethod("GetValueOrDefault", Type.EmptyTypes)!);
            return underlying.IsEnum ? Expression.Convert(value, underlying.GetEnumUnderlyingType()) : value;
        }

        return access;
    }

    /// <summary>
    /// Builds a boxed-value normalizer for query inputs: a user-supplied field value of the declared
    /// member type (e.g. an enum literal, or a Nullable&lt;DateTime&gt;) is converted to the index's
    /// normalized storage representation. Identity (no allocation) when no normalization is needed.
    /// </summary>
    internal static Func<IData, IData> BuildValueNormalizer(Type rawType)
    {
        if (NormalizeStorageType(rawType) == rawType)
            return v => v;

        var input     = Expression.Parameter(typeof(object), "value");
        var typed     = Expression.Convert(input, rawType);
        var normalized = NormalizeStorageValue(typed);
        var boxed     = Expression.Convert(normalized, typeof(object));
        return Expression.Lambda<Func<IData, IData>>(boxed, input).Compile();
    }

    /// <summary>Declared CLR type of an indexed record member (before normalization).</summary>
    internal static Type GetRecordMemberRawType(Type recordType, int slotIndex)
        => GetMemberType(GetSlotMember(recordType, slotIndex));

    /// <summary>Normalized index-storage CLR type of an indexed record member.</summary>
    internal static Type GetRecordMemberStorageType(Type recordType, int slotIndex)
        => NormalizeStorageType(GetRecordMemberRawType(recordType, slotIndex));

    private static MemberInfo GetSlotMember(Type recordType, int index)
    {
        var slotField = recordType.GetField($"Slot{index}");
        if (slotField != null)
            return slotField;

        var members = DataTypeUtils.GetPublicMembers(recordType).ToArray();
        if (index < 0 || index >= members.Length)
            throw new ArgumentException($"Slot index {index} is out of range for type '{recordType.Name}' which has {members.Length} members.");
        return members[index];
    }

    private static Expression AccessMember(Expression instance, MemberInfo member)
    {
        return member switch
        {
            FieldInfo fi   => Expression.Field(instance, fi),
            PropertyInfo pi => Expression.Property(instance, pi),
            _ => throw new ArgumentException($"Unexpected member type: {member.MemberType}")
        };
    }

    private static Type GetMemberType(MemberInfo member)
    {
        return member switch
        {
            FieldInfo fi   => fi.FieldType,
            PropertyInfo pi => pi.PropertyType,
            _ => throw new ArgumentException($"Unexpected member type: {member.MemberType}")
        };
    }
}
