// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using System.Diagnostics;

namespace CatDb.General.Extensions;
public static class StopwatchExtensions
{
    public static double GetSpeed(this Stopwatch sw, long count)
    {
        return count / (sw.ElapsedMilliseconds / 1000.0);
    }
}
