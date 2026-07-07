// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using System.Collections.Concurrent;
using CatDb.Data;
using CatDb.General.Persist;

namespace CatDb.WaterfallTree;
public class TypeEngine
{
    private static readonly ConcurrentDictionary<Type, TypeEngine> Map = new();

    public IComparer<IData>? Comparer { get; set; }
    public IEqualityComparer<IData>? EqualityComparer { get; set; }
    public IPersist<IData>? Persist { get; set; }
    public IIndexerPersist<IData>? IndexerPersist { get; set; }

    private static TypeEngine Create(Type type)
    {
        var descriptor = new TypeEngine
        {
            Persist = new DataPersist(type, null, AllowNull.OnlyMembers)
        };

        if (DataTypeUtils.IsAllComparable(type))
        {
            descriptor.Comparer = new DataComparer(type);
            descriptor.EqualityComparer = new DataEqualityComparer(type);

            // IndexerPersist only for pure primitives — Guid (bare or as a composite slot,
            // e.g. a non-unique index key over a Guid-keyed table) has no DataIndexerPersist.
            if (DataTypeUtils.IsAllPrimitive(type) && type != typeof(Guid))
                descriptor.IndexerPersist = new DataIndexerPersist(type);
        }

        return descriptor;
    }

    public static TypeEngine Default(Type type)
    {
        return Map.GetOrAdd(type, Create(type));
    }
}
