// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.General.Threading;
public class Countdown
{
    private long _count; 
    
    public void Increment()
    {
        Interlocked.Increment(ref _count);
    }

    public void Decrement()
    {
        Interlocked.Decrement(ref _count);
    }

    public void Wait()
    {
        SpinWait.SpinUntil(() => Count == 0);
    }

    public Task WaitAsync(CancellationToken ct = default) =>
        Task.Run(() => SpinWait.SpinUntil(() => Count == 0), ct);

    public long Count => Interlocked.Read(ref _count);
}
