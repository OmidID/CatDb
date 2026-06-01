// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿namespace CatDb.General.Comparers;
public class ComparerInvertor<T> : IComparer<T>
{
    public readonly IComparer<T> Comparer;

    public ComparerInvertor(IComparer<T> comparer)
    {
        Comparer = comparer;
    }

    public int Compare(T? x, T? y)
    {
        return -Comparer.Compare(x!, y!);
    }
}
