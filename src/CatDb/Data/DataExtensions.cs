// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

// DataExtensions.Value() accessed the Data<T>.Value field in expression trees.
// With IData = object and Data<T> removed from the hot path, expression trees now use
// Expression.Convert(expr, targetType) directly. Extension removed.
