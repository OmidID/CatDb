// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.Database.Operations;

public static class OperationCode
{
    public const int UNDEFINED     = 0;
    public const int REPLACE       = 1;
    public const int INSERT_OR_IGNORE = 2;
    public const int DELETE        = 3;
    public const int DELETE_RANGE  = 4;
    public const int CLEAR         = 5;
    public const int MAX           = 256;
}
