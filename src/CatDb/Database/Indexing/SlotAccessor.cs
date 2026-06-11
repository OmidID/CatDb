// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using System.Reflection;
using CatDb.Data;

namespace CatDb.Database.Indexing;

/// <summary>
/// Compiles efficient delegates to extract slot values from IData records
/// and to build IData keys for index tables.
///
/// At index creation time, we inspect the record's concrete Type (from Locator.RecordType)
/// and compile expression trees that directly access Slot0, Slot1, ... or property members
/// without per-call reflection. This gives us near-zero-overhead slot extraction.
///
/// Supports both:
/// - Slots&lt;T0,T1,...&gt; types (used with OpenXTablePortable)
/// - User classes like Customer (used with OpenXTable&lt;TKey, TRecord&gt;)
/// </summary>
internal static class SlotAccessor
{
    /// <summary>
    /// Creates a delegate that extracts one or more slots from an IData record
    /// and produces an IData value suitable as an index table key.
    ///
    /// For a single slot index: returns Data&lt;SlotType&gt; wrapping the slot value.
    /// For a composite index: returns Data&lt;Slots&lt;T0,T1,...&gt;&gt; wrapping the slot values.
    /// </summary>
    /// <param name="recordType">The concrete record type (e.g. Slots&lt;string, int, double&gt;).</param>
    /// <param name="slotIndices">Which slots to extract (0-based).</param>
    /// <returns>A compiled delegate: IData record → IData indexKey.</returns>
    internal static Func<IData, IData> BuildExtractor(Type recordType, int[] slotIndices)
    {
        if (slotIndices.Length == 0)
            throw new ArgumentException("Must specify at least one slot index.");

        // The IData record is actually Data<RecordType>.
        // We need: ((Data<RecordType>)input).Value.Slot{i}
        var dataType = typeof(Data<>).MakeGenericType(recordType);
        var valueField = dataType.GetField("Value")!;

        var inputParam = Expression.Parameter(typeof(IData), "record");
        var castToData = Expression.Convert(inputParam, dataType);
        var valueAccess = Expression.Field(castToData, valueField);

        if (slotIndices.Length == 1)
        {
            // Single slot → Data<SlotType>
            var slotMember = GetSlotMember(recordType, slotIndices[0]);
            var slotType = GetMemberType(slotMember);
            var slotAccess = AccessMember(valueAccess, slotMember);

            var resultDataType = typeof(Data<>).MakeGenericType(slotType);
            var ctor = resultDataType.GetConstructor([slotType])!;
            var newData = Expression.New(ctor, slotAccess);
            var castResult = Expression.Convert(newData, typeof(IData));

            return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
        }
        else
        {
            // Multiple slots → Data<Slots<T0, T1, ...>>
            var slotMembers = slotIndices.Select(i => GetSlotMember(recordType, i)).ToArray();
            var slotTypes = slotMembers.Select(GetMemberType).ToArray();
            var slotsType = SlotsBuilder.BuildType(slotTypes);

            var slotAccesses = slotMembers.Select(m => AccessMember(valueAccess, m)).ToArray();
            var slotsCtor = slotsType.GetConstructors()
                .First(c => c.GetParameters().Length == slotTypes.Length);
            var newSlots = Expression.New(slotsCtor, slotAccesses);

            var resultDataType = typeof(Data<>).MakeGenericType(slotsType);
            var dataCtor = resultDataType.GetConstructor([slotsType])!;
            var newData = Expression.New(dataCtor, newSlots);
            var castResult = Expression.Convert(newData, typeof(IData));

            return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
        }
    }

    /// <summary>
    /// Builds the DataType for the index table key, given the record DataType and slot indices.
    /// Single slot → the slot's DataType.
    /// Multiple slots → DataType.Slots(slot0Type, slot1Type, ...).
    /// </summary>
    internal static DataType BuildIndexKeyDataType(DataType recordDataType, int[] slotIndices)
    {
        if (recordDataType.IsPrimitive)
        {
            // Primitive record (single value, not slots) — slot 0 is the value itself
            if (slotIndices.Length == 1 && slotIndices[0] == 0)
                return recordDataType;
            throw new ArgumentException("Primitive record type only supports slot index 0.");
        }

        if (!recordDataType.IsSlots)
            throw new ArgumentException("Record DataType must be Slots or Primitive for indexing.");

        if (slotIndices.Length == 1)
            return recordDataType[slotIndices[0]];

        var types = slotIndices.Select(i => recordDataType[i]).ToArray();
        return DataType.Slots(types);
    }

    /// <summary>
    /// Builds the DataType for a non-unique index table key:
    /// Slots(indexed_field_types..., primary_key_type).
    /// The primary key is appended to ensure uniqueness in the W-tree.
    /// </summary>
    internal static DataType BuildNonUniqueIndexKeyDataType(DataType recordDataType, int[] slotIndices, DataType primaryKeyDataType)
    {
        var fieldTypes = slotIndices.Select(i =>
            recordDataType.IsPrimitive ? recordDataType : recordDataType[i]).ToArray();

        var allTypes = new DataType[fieldTypes.Length + 1];
        Array.Copy(fieldTypes, allTypes, fieldTypes.Length);
        allTypes[^1] = primaryKeyDataType;
        return DataType.Slots(allTypes);
    }

    /// <summary>
    /// For a non-unique index, builds a composite key extractor that appends
    /// the primary key to the indexed field values.
    /// Result: Data&lt;Slots&lt;field0, field1, ..., primaryKey&gt;&gt;
    /// </summary>
    internal static Func<IData, IData, IData> BuildNonUniqueKeyBuilder(
        Type recordType, int[] slotIndices, Type primaryKeyType)
    {
        var dataRecordType = typeof(Data<>).MakeGenericType(recordType);
        var dataKeyType = typeof(Data<>).MakeGenericType(primaryKeyType);

        var recordParam = Expression.Parameter(typeof(IData), "record");
        var keyParam = Expression.Parameter(typeof(IData), "key");

        var castRecord = Expression.Convert(recordParam, dataRecordType);
        var recordValue = Expression.Field(castRecord, dataRecordType.GetField("Value")!);

        var castKey = Expression.Convert(keyParam, dataKeyType);
        var keyValue = Expression.Field(castKey, dataKeyType.GetField("Value")!);

        // Get slot field types + primary key type
        Expression[] slotAccesses;

        MemberInfo[] slotMembers;

        if (DataType.IsPrimitiveType(recordType))
        {
            // Primitive record — the value itself is the only "slot"
            slotMembers = [];
            slotAccesses = [recordValue];
        }
        else
        {
            slotMembers = slotIndices.Select(i => GetSlotMember(recordType, i)).ToArray();
            slotAccesses = slotMembers.Select(m => AccessMember(recordValue, m)).ToArray();
        }

        var slotTypes = slotAccesses.Select(a => a.Type).ToArray();
        var allTypes = new Type[slotTypes.Length + 1];
        Array.Copy(slotTypes, allTypes, slotTypes.Length);
        allTypes[^1] = primaryKeyType;

        var compositeType = SlotsBuilder.BuildType(allTypes);
        var compositeCtor = compositeType.GetConstructors()
            .First(c => c.GetParameters().Length == allTypes.Length);

        var allAccesses = new Expression[slotAccesses.Length + 1];
        Array.Copy(slotAccesses, allAccesses, slotAccesses.Length);
        allAccesses[^1] = keyValue;

        var newComposite = Expression.New(compositeCtor, allAccesses);

        var resultDataType = typeof(Data<>).MakeGenericType(compositeType);
        var dataCtor = resultDataType.GetConstructor([compositeType])!;
        var newData = Expression.New(dataCtor, newComposite);
        var castResult = Expression.Convert(newData, typeof(IData));

        return Expression.Lambda<Func<IData, IData, IData>>(castResult, recordParam, keyParam).Compile();
    }

    /// <summary>
    /// For a non-unique index, extracts the field portion from a composite key
    /// (strips the primary key suffix). Used for comparison during scans.
    /// </summary>
    internal static Func<IData, IData> BuildFieldExtractorFromCompositeKey(
        Type compositeKeyType, int fieldCount)
    {
        var dataType = typeof(Data<>).MakeGenericType(compositeKeyType);
        var inputParam = Expression.Parameter(typeof(IData), "compositeKey");
        var castToData = Expression.Convert(inputParam, dataType);
        var valueAccess = Expression.Field(castToData, dataType.GetField("Value")!);

        if (fieldCount == 1)
        {
            // Single field → extract Slot0
            var slotMember = GetSlotMember(compositeKeyType, 0);
            var slotType = GetMemberType(slotMember);
            var slotAccess = AccessMember(valueAccess, slotMember);
            var resultType = typeof(Data<>).MakeGenericType(slotType);
            var ctor = resultType.GetConstructor([slotType])!;
            var newData = Expression.New(ctor, slotAccess);
            var castResult = Expression.Convert(newData, typeof(IData));
            return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
        }
        else
        {
            // Multiple fields → extract Slot0..Slot{fieldCount-1} into a new Slots
            var members = Enumerable.Range(0, fieldCount)
                .Select(i => GetSlotMember(compositeKeyType, i)).ToArray();
            var types = members.Select(GetMemberType).ToArray();
            var slotsType = SlotsBuilder.BuildType(types);

            var accesses = members.Select(m => AccessMember(valueAccess, m)).ToArray();
            var slotsCtor = slotsType.GetConstructors()
                .First(c => c.GetParameters().Length == types.Length);
            var newSlots = Expression.New(slotsCtor, accesses);

            var resultType = typeof(Data<>).MakeGenericType(slotsType);
            var dataCtor = resultType.GetConstructor([slotsType])!;
            var newData = Expression.New(dataCtor, newSlots);
            var castResult = Expression.Convert(newData, typeof(IData));
            return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
        }
    }

    /// <summary>
    /// Extracts the primary key (last slot) from a non-unique composite index key.
    /// </summary>
    internal static Func<IData, IData> BuildPrimaryKeyExtractorFromCompositeKey(
        Type compositeKeyType, int totalSlotCount)
    {
        var dataType = typeof(Data<>).MakeGenericType(compositeKeyType);
        var inputParam = Expression.Parameter(typeof(IData), "compositeKey");
        var castToData = Expression.Convert(inputParam, dataType);
        var valueAccess = Expression.Field(castToData, dataType.GetField("Value")!);

        // Primary key is the last slot
        var pkMember = GetSlotMember(compositeKeyType, totalSlotCount - 1);
        var pkType = GetMemberType(pkMember);
        var pkAccess = AccessMember(valueAccess, pkMember);

        var resultType = typeof(Data<>).MakeGenericType(pkType);
        var ctor = resultType.GetConstructor([pkType])!;
        var newData = Expression.New(ctor, pkAccess);
        var castResult = Expression.Convert(newData, typeof(IData));
        return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
    }

    /// <summary>
    /// Builds a delegate that creates a "from" composite key for non-unique index scans.
    /// Given a field value IData, appends a default primary key to form the composite.
    /// </summary>
    internal static Func<IData, IData> BuildScanFromKeyBuilder(
        Type compositeKeyType, Type fieldType, Type primaryKeyType, int fieldCount)
    {
        var inputParam = Expression.Parameter(typeof(IData), "fieldValue");

        // We need to build: new Data<CompositeType>(new CompositeType(field0, ..., default(PK)))
        var totalSlots = fieldCount + 1;

        Expression[] ctorArgs = new Expression[totalSlots];

        if (fieldCount == 1)
        {
            // fieldValue is Data<T> — extract the value
            var dataFieldType = typeof(Data<>).MakeGenericType(fieldType);
            var castInput = Expression.Convert(inputParam, dataFieldType);
            var fieldValue = Expression.Field(castInput, dataFieldType.GetField("Value")!);
            ctorArgs[0] = fieldValue;
        }
        else
        {
            // fieldValue is Data<Slots<T0,T1,...>> — extract each slot
            var dataFieldType = typeof(Data<>).MakeGenericType(fieldType);
            var castInput = Expression.Convert(inputParam, dataFieldType);
            var slotsValue = Expression.Field(castInput, dataFieldType.GetField("Value")!);
            for (int i = 0; i < fieldCount; i++)
            {
                var slotMember = GetSlotMember(fieldType, i);
                ctorArgs[i] = AccessMember(slotsValue, slotMember);
            }
        }

        // Append default primary key
        ctorArgs[^1] = Expression.Default(primaryKeyType);

        var compositeCtor = compositeKeyType.GetConstructors()
            .First(c => c.GetParameters().Length == totalSlots);
        var newComposite = Expression.New(compositeCtor, ctorArgs);

        var resultDataType = typeof(Data<>).MakeGenericType(compositeKeyType);
        var dataCtor = resultDataType.GetConstructor([compositeKeyType])!;
        var newData = Expression.New(dataCtor, newComposite);
        var castResult = Expression.Convert(newData, typeof(IData));

        return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
    }

    /// <summary>
    /// Builds a compiled equality comparer for IData field values.
    /// Used to detect when scanning past the target field in non-unique index scans.
    /// For Slots types (which don't override Equals), performs structural comparison.
    /// </summary>
    internal static Func<IData, IData, bool> BuildFieldEqualityComparer(Type fieldType)
    {
        var dataType = typeof(Data<>).MakeGenericType(fieldType);
        var aParam = Expression.Parameter(typeof(IData), "a");
        var bParam = Expression.Parameter(typeof(IData), "b");

        var castA = Expression.Convert(aParam, dataType);
        var castB = Expression.Convert(bParam, dataType);
        var valueA = Expression.Field(castA, dataType.GetField("Value")!);
        var valueB = Expression.Field(castB, dataType.GetField("Value")!);

        Expression equalsExpr;

        if (typeof(ISlots).IsAssignableFrom(fieldType))
        {
            // Structural comparison: compare each slot field individually
            var members = DataTypeUtils.GetPublicMembers(fieldType).ToArray();
            Expression? combined = null;
            foreach (var member in members)
            {
                var memberType = GetMemberType(member);
                var accessA = AccessMember(valueA, member);
                var accessB = AccessMember(valueB, member);

                var slotComparerType = typeof(System.Collections.Generic.EqualityComparer<>).MakeGenericType(memberType);
                var slotDefaultProp = slotComparerType.GetProperty("Default", BindingFlags.Static | BindingFlags.Public)!;
                var slotEqualsMethod = slotComparerType.GetMethod("Equals", [memberType, memberType])!;
                var slotComparer = Expression.Property(null, slotDefaultProp);
                var slotEquals = Expression.Call(slotComparer, slotEqualsMethod, accessA, accessB);

                combined = combined == null ? slotEquals : Expression.AndAlso(combined, slotEquals);
            }
            equalsExpr = combined ?? Expression.Constant(true);
        }
        else
        {
            // Use EqualityComparer<T>.Default.Equals(a, b)
            var comparerType = typeof(System.Collections.Generic.EqualityComparer<>).MakeGenericType(fieldType);
            var defaultProp = comparerType.GetProperty("Default", BindingFlags.Static | BindingFlags.Public)!;
            var equalsMethod = comparerType.GetMethod("Equals", [fieldType, fieldType])!;

            var comparer = Expression.Property(null, defaultProp);
            equalsExpr = Expression.Call(comparer, equalsMethod, valueA, valueB);
        }

        return Expression.Lambda<Func<IData, IData, bool>>(equalsExpr, aParam, bParam).Compile();
    }

    /// <summary>The .NET type of slot <paramref name="slotIndex"/> in a composite key/record type.</summary>
    internal static Type GetSlotType(Type compositeKeyType, int slotIndex)
        => GetMemberType(GetSlotMember(compositeKeyType, slotIndex));

    /// <summary>
    /// Builds a seek key for a composite-index <b>prefix</b> scan: given the leading prefix value
    /// (Data&lt;prefixType&gt;), produces the composite key (prefix…, default, …, default) used as
    /// the inclusive lower bound. Relies on the same "default == minimum" assumption as
    /// <see cref="BuildScanFromKeyBuilder"/>; correct for non-negative trailing slots.
    /// </summary>
    internal static Func<IData, IData> BuildPrefixSeekKeyBuilder(Type compositeKeyType, Type prefixType, int prefixLen)
    {
        var inputParam = Expression.Parameter(typeof(IData), "prefix");

        var ctor = compositeKeyType.GetConstructors()
            .OrderByDescending(c => c.GetParameters().Length).First();
        var ps = ctor.GetParameters();
        var args = new Expression[ps.Length];

        var dataPrefixType = typeof(Data<>).MakeGenericType(prefixType);
        var castInput = Expression.Convert(inputParam, dataPrefixType);
        var prefixValue = Expression.Field(castInput, dataPrefixType.GetField("Value")!);

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
        var resultDataType = typeof(Data<>).MakeGenericType(compositeKeyType);
        var dataCtor = resultDataType.GetConstructor([compositeKeyType])!;
        var newData = Expression.New(dataCtor, newComposite);
        var castResult = Expression.Convert(newData, typeof(IData));
        return Expression.Lambda<Func<IData, IData>>(castResult, inputParam).Compile();
    }

    private static MemberInfo GetSlotMember(Type recordType, int index)
    {
        // For Slots<T0,T1,...> types, use Slot{i} fields directly
        var slotField = recordType.GetField($"Slot{index}");
        if (slotField != null)
            return slotField;

        // For user classes (Customer, etc.), get the i-th public read/write member
        var members = DataTypeUtils.GetPublicMembers(recordType).ToArray();
        if (index < 0 || index >= members.Length)
            throw new ArgumentException($"Slot index {index} is out of range for type '{recordType.Name}' which has {members.Length} members.");
        return members[index];
    }

    private static Expression AccessMember(Expression instance, MemberInfo member)
    {
        return member switch
        {
            FieldInfo fi => Expression.Field(instance, fi),
            PropertyInfo pi => Expression.Property(instance, pi),
            _ => throw new ArgumentException($"Unexpected member type: {member.MemberType}")
        };
    }

    private static Type GetMemberType(MemberInfo member)
    {
        return member switch
        {
            FieldInfo fi => fi.FieldType,
            PropertyInfo pi => pi.PropertyType,
            _ => throw new ArgumentException($"Unexpected member type: {member.MemberType}")
        };
    }
}
