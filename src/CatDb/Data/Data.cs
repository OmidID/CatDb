// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.Data;
public class Data<T> : IData
{
    public T Value = default!;

    public Data()
    {
    }

    public Data(T value)
    {
        Value = value;
    }

    public override string ToString()
    {
        return Value?.ToString() ?? string.Empty;
    }
}
