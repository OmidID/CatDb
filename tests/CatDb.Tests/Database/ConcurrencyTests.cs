// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Concurrent write/read stress tests. These cover scenarios that caused crashes
/// when Thread.Abort() was active or when data structures had race conditions.
/// Uses Task.Run to exercise multi-threaded access patterns.
/// </summary>
public class ConcurrencyTests : IDisposable
{
    private readonly IStorageEngine _engine;

    public ConcurrencyTests()
    {
        _engine = CatDb.Database.CatDb.FromMemory();
    }

    public void Dispose() => _engine.Dispose();

    [Fact]
    public async Task ConcurrentInserts_AllRecordsWritten()
    {
        const int threads = 4;
        const int perThread = 500;
        var table = _engine.OpenXTable<long, string>("concurrent");

        var tasks = Enumerable.Range(0, threads).Select(t =>
            Task.Run(() =>
            {
                var offset = (long)(t * perThread);
                for (var i = 0L; i < perThread; i++)
                    table[offset + i] = $"value-{offset + i}";
            })).ToArray();

        await Task.WhenAll(tasks);
        _engine.Commit();

        table.Count().Should().Be(threads * perThread);
    }

    [Fact]
    public async Task ConcurrentReads_NoExceptions()
    {
        var table = _engine.OpenXTable<long, string>("readonly");
        for (var i = 0L; i < 1000L; i++)
            table[i] = $"v{i}";
        _engine.Commit();

        var tasks = Enumerable.Range(0, 8).Select(_ =>
            Task.Run(() =>
            {
                var rng = new Random();
                for (var i = 0; i < 200; i++)
                {
                    var key = (long)rng.Next(0, 1000);
                    table.TryGet(key, out string? _);
                }
            })).ToArray();

        var act = async () => await Task.WhenAll(tasks);
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ConcurrentForwardScans_NoExceptions()
    {
        var table = _engine.OpenXTable<long, string>("scan");
        for (var i = 0L; i < 500L; i++) table[i] = $"v{i}";
        _engine.Commit();

        var tasks = Enumerable.Range(0, 4).Select(_ =>
            Task.Run(() =>
            {
                foreach (var _ in table.Forward())
                {
                    // just iterate
                }
            })).ToArray();

        var act = async () => await Task.WhenAll(tasks);
        await act.Should().NotThrowAsync();
    }
}
