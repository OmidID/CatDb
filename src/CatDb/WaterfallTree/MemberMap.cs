// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.General.Extensions;

namespace CatDb.WaterfallTree;

/// <summary>
/// A recursive slot-index → member-name map that mirrors the shape of a
/// <see cref="DataType"/>.  Unlike a flat <c>Dictionary&lt;string,int&gt;</c> it
/// descends into <b>nested Slots</b> (object members) and <b>collection element</b>
/// types, so deeply-nested record schemas keep their real field names instead of
/// degrading to <c>Slot0/Slot1</c>.
///
/// • <see cref="Names"/>       – name → slot index at this Slots level.
/// • <see cref="Children"/>    – slot index → nested map (when that slot is itself Slots).
/// • <see cref="Element"/>     – element map for an Array/List slot whose element is Slots.
///
/// Exactly one of (<see cref="Names"/>/<see cref="Children"/>) or <see cref="Element"/>
/// is meaningful for a given node, matching whether the node is a Slots or a collection.
/// </summary>
public sealed class MemberMap
{
    private static readonly IReadOnlyDictionary<int, MemberMap> NoChildren =
        new Dictionary<int, MemberMap>();

    public IReadOnlyDictionary<string, int> Names { get; }
    public IReadOnlyDictionary<int, MemberMap> Children { get; }
    public MemberMap? Element { get; }

    public MemberMap(
        IReadOnlyDictionary<string, int> names,
        IReadOnlyDictionary<int, MemberMap>? children = null,
        MemberMap? element = null)
    {
        Names = names ?? new Dictionary<string, int>();
        Children = children ?? NoChildren;
        Element = element;
    }

    /// <summary>Reverse lookup: slot index → name (empty for collection nodes).</summary>
    public IReadOnlyDictionary<int, string> NamesByIndex()
    {
        var map = new Dictionary<int, string>(Names.Count);
        foreach (var kv in Names)
            map[kv.Value] = kv.Key;
        return map;
    }

    // ── Build from CLR type, driven by the matching DataType ──────────────────

    /// <summary>
    /// Builds a recursive member map from a concrete CLR <paramref name="type"/>,
    /// using <paramref name="dataType"/> to decide where Slots/collections occur.
    /// Returns null for primitive or anonymous-tuple nodes (nothing to name).
    /// </summary>
    public static MemberMap? Build(DataType dataType, Type? type)
    {
        if (dataType.IsPrimitive || type == null)
            return null;

        if (dataType.IsArray || dataType.IsList)
        {
            var elemType = ElementClrType(type);
            var elem = Build(dataType[0], elemType);
            return elem == null ? null : new MemberMap(new Dictionary<string, int>(), null, elem);
        }

        if (dataType.IsDictionary)
            return null;   // dictionaries are keyed by value, not by named members

        if (!dataType.IsSlots)
            return null;

        // Nullable<T> is modelled as Slots(T) with a single anonymous slot — skip naming.
        if (type.IsNullable())
            return null;

        var members = DataTypeUtils.GetPublicMembers(type).ToArray();
        if (members.Length != dataType.TypesCount)
            return null;   // anonymous tuple / shape mismatch — no reliable names

        var names = new Dictionary<string, int>(members.Length);
        var children = new Dictionary<int, MemberMap>();
        for (var i = 0; i < members.Length; i++)
        {
            names[members[i].Name] = i;
            var child = Build(dataType[i], members[i].GetPropertyOrFieldType());
            if (child != null)
                children[i] = child;
        }

        return new MemberMap(names, children.Count > 0 ? children : null);
    }

    private static Type? ElementClrType(Type type)
    {
        if (type.IsArray)
            return type.GetElementType();
        if (type.IsList())
            return type.GetGenericArguments()[0];
        return null;
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    public void Serialize(BinaryWriter writer)
    {
        writer.Write(Names.Count);
        foreach (var kv in Names)
        {
            writer.Write(kv.Key);
            writer.Write(kv.Value);
        }

        writer.Write(Children.Count);
        foreach (var kv in Children)
        {
            writer.Write(kv.Key);
            kv.Value.Serialize(writer);
        }

        if (Element == null)
            writer.Write(false);
        else
        {
            writer.Write(true);
            Element.Serialize(writer);
        }
    }

    public static MemberMap Deserialize(BinaryReader reader)
    {
        var nameCount = reader.ReadInt32();
        var names = new Dictionary<string, int>(nameCount);
        for (var i = 0; i < nameCount; i++)
        {
            var key = reader.ReadString();
            var value = reader.ReadInt32();
            names[key] = value;
        }

        var childCount = reader.ReadInt32();
        Dictionary<int, MemberMap>? children = null;
        if (childCount > 0)
        {
            children = new Dictionary<int, MemberMap>(childCount);
            for (var i = 0; i < childCount; i++)
            {
                var idx = reader.ReadInt32();
                children[idx] = Deserialize(reader);
            }
        }

        var element = reader.ReadBoolean() ? Deserialize(reader) : null;

        return new MemberMap(names, children, element);
    }
}
