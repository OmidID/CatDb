// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using System.Linq.Expressions;

namespace CatDb.Data;
public static class DataExtensions
{
    public static Expression Value(this Expression data)
    {
        return Expression.Field(data, "Value");
    }
}
