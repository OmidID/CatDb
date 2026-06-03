// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.Database;

public static class StructureType
{
    // do not change
    public const int RESERVED = 0;
    public const int XTABLE   = 1;
    public const int XFILE    = 2;

    public static bool IsValid(int type) => type is XTABLE or XFILE;
}
