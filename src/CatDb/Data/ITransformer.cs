// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.Data;
public interface ITransformer<T1, T2>
{
    T2 To(T1 value1);
    T1 From(T2 value2);
}
