// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using CatDb.Data;

namespace CatDb.Remote.Commands;

/// <summary>
/// Serializes index field/prefix values (whose type differs from the table's key/record types) to
/// and from raw bytes for the wire, using a <see cref="DataPersist"/> built from the value's type.
/// IData is a global alias for <see cref="object"/> — the client serializes using the value's own
/// runtime type directly (a boxed primitive or a composite <c>Slots&lt;...&gt;</c>, never wrapped);
/// the server deserializes using the index's resolved field type — both default-configured, so the
/// encodings match.
/// </summary>
internal static class RemoteFieldCodec
{
    private static readonly ConcurrentDictionary<Type, DataPersist> Cache = new();

    private static DataPersist Persist(Type type) => Cache.GetOrAdd(type, t => new DataPersist(t));

    /// <summary>Serializes an index value using its own runtime type as the field type.</summary>
    public static byte[] Serialize(IData value)
        => Serialize(value, value.GetType());

    public static byte[] Serialize(IData value, Type type)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        Persist(type).Write(bw, value);
        bw.Flush();
        return ms.ToArray();
    }

    public static IData Deserialize(byte[] bytes, Type type)
    {
        using var ms = new MemoryStream(bytes);
        using var br = new BinaryReader(ms);
        return Persist(type).Read(br);
    }
}
