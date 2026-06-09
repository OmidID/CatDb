// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Remote;

/// <summary>
/// Connection-level tuning for how a remote client pages <see cref="XTableRemote"/>
/// Forward/Backward scans over the wire. Set on the client (or via
/// <c>CatDb.FromNetwork(..., scanOptions)</c>) — nothing here is hard-coded per table.
///
/// <para><b>Unbounded scans</b> start at <see cref="InitialPageCapacity"/> rows per round-trip and
/// grow ×<see cref="PageGrowthFactor"/> up to <see cref="MaxPageCapacity"/>, so a full scan reaches
/// high throughput in a few hops while a short consumer never drags the whole range over the wire.</para>
///
/// <para><b>Bounded scans</b> (<c>.Take(n)</c> / cursor paging) push the exact limit to the server:
/// the engine seeks and returns only the requested rows in a single round-trip — no over-fetch,
/// no client-side discard.</para>
/// </summary>
public sealed class RemoteScanOptions
{
    private int _initialPageCapacity = 1024;
    private int _maxPageCapacity     = 100_000;
    private int _pageGrowthFactor    = 8;

    /// <summary>Rows requested on the first round-trip of an unbounded scan. Default 1024.</summary>
    public int InitialPageCapacity
    {
        get => _initialPageCapacity;
        set => _initialPageCapacity = value < 1 ? 1 : value;
    }

    /// <summary>Upper bound on rows per round-trip as pages grow. Default 100,000.</summary>
    public int MaxPageCapacity
    {
        get => _maxPageCapacity;
        set => _maxPageCapacity = value < 1 ? 1 : value;
    }

    /// <summary>Multiplier applied to the page size after each round-trip. Default 8 (min 2).</summary>
    public int PageGrowthFactor
    {
        get => _pageGrowthFactor;
        set => _pageGrowthFactor = value < 2 ? 2 : value;
    }

    /// <summary>Clamps a desired capacity into [1, <see cref="MaxPageCapacity"/>].</summary>
    internal int Clamp(int capacity)
    {
        if (capacity < 1) return 1;
        return capacity > _maxPageCapacity ? _maxPageCapacity : capacity;
    }
}
