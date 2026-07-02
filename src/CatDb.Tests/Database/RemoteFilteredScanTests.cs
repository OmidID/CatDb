// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Net;
using System.Net.Sockets;
using CatDb.Database;
using CatDb.Extensions;
using CatDb.General.Communication;
using CatDb.Remote;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Regression for a 2026-06 field report: a filtered, unbounded-upper-range key scan
/// (<c>KeyQuery.AtLeast(x).WithFilter(...)</c> / <c>AtMost(x).WithFilter(...)</c>) against a REMOTE table
/// silently ignored the filter and returned every row. Root cause: <c>XTablePortable&lt;TKey,TRecord&gt;
/// .ScanTake</c>/<c>ScanBackwardTake</c> has a "remote fast path" that pushes the row-limit to the server via
/// <c>IRemoteScanTable.ForwardTake</c>/<c>BackwardTake</c> for a single-round-trip no-over-fetch optimization.
/// That path assumed a filter would always have been intercepted by the LOCAL segment-scan branch above it
/// (<c>Table is XTablePortable</c>) — but that check only ever matches the in-process table, never
/// <c>XTableRemote</c>, so a filtered query with no opposite bound fell straight through to the fast path
/// and the filter closure was never evaluated. Only reproduces over the real wire — an in-process WTree
/// scan under equivalent concurrent load never hit this (confirmed: the bug is a deterministic logic gap,
/// not a race). Exercises the exact <see cref="StorageEngineServer"/> + TCP client stack, matching the
/// codebase's established remote-test pattern (see <see cref="RemoteIndexTests"/>).
/// </summary>
public class RemoteFilteredScanTests
{
    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    [Fact]
    public async Task ForwardTake_WithFilter_NoUpperBound_NeverReturnsFilteredOutRows()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p");
            var table = client.OpenXTable<long, string>("ticks");

            const int n = 2_000;
            for (var i = 0L; i < n; i++) table[i] = $"v{i}";
            client.Commit();

            // AtLeast (no upper bound) + WithFilter — exactly the shape that hit the buggy remote fast path.
            var q = KeyQuery<long>.AtLeast(n / 2).WithFilter(k => k % 2 == 0);
            var got = table.QueryTake(q, 300).ToList();

            got.Should().NotBeEmpty();
            got.Should().OnlyContain(kv => kv.Key % 2 == 0, "the filter must be applied over the wire, not skipped");
            got.Should().OnlyContain(kv => kv.Key >= n / 2);

            var expected = Enumerable.Range((int)(n / 2), (int)(n - n / 2))
                .Select(i => (long)i).Where(k => k % 2 == 0).Take(300).ToList();
            got.Select(kv => kv.Key).Should().Equal(expected);
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task BackwardTake_WithFilter_NoLowerBound_NeverReturnsFilteredOutRows()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p");
            var table = client.OpenXTable<long, string>("ticks");

            const int n = 2_000;
            for (var i = 0L; i < n; i++) table[i] = $"v{i}";
            client.Commit();

            // AtMost (no lower bound) + WithFilter — the backward-direction mirror of the same bug.
            var q = KeyQuery<long>.AtMost(n / 2).WithFilter(k => k % 2 == 0);
            var got = table.QueryBackwardTake(q, 300).ToList();

            got.Should().NotBeEmpty();
            got.Should().OnlyContain(kv => kv.Key % 2 == 0, "the filter must be applied over the wire, not skipped");
            got.Should().OnlyContain(kv => kv.Key <= n / 2);

            var expected = Enumerable.Range(0, (int)(n / 2) + 1)
                .Select(i => (long)i).Where(k => k % 2 == 0).Reverse().Take(300).ToList();
            got.Select(kv => kv.Key).Should().Equal(expected);
        }
        finally
        {
            await server.StopAsync();
        }
    }
}
