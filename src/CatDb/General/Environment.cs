// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.General;
public static class Environment
{
    public static readonly bool RunningOnMono = Type.GetType("Mono.Runtime") != null;
}
