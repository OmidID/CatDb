// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.General.Comparers;
public class KeyValuePairComparer<TKey, TValue> : IComparer<KeyValuePair<TKey, TValue>>
{
    public static readonly KeyValuePairComparer<TKey, TValue> Instance = new(Comparer<TKey>.Default);

    public IComparer<TKey> Comparer { get; private set; }

    public KeyValuePairComparer(IComparer<TKey> comparer)
    {
        Comparer = comparer;
    }

    public int Compare(KeyValuePair<TKey, TValue> x, KeyValuePair<TKey, TValue> y)
    {
        return Comparer.Compare(x.Key, y.Key);
    }
}
