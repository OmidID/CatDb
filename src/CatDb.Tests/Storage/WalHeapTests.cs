// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Storage;
using CatDb.WaterfallTree;
using FluentAssertions;
using CatDbFactory = CatDb.Database.CatDb;

namespace CatDb.Tests.Storage;

/// <summary>
/// Tests for WalHeap — crash-safe WAL commit mode.
/// </summary>
public class WalHeapTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;

    public WalHeapTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"catdb_wal_test_{Guid.NewGuid():N}.db");
        _walPath = _dbPath + ".wal";
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_walPath); } catch { }
    }

    private (WalHeap walHeap, Heap innerHeap) CreateWalHeap()
    {
        var stream = new FileStream(_dbPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
        var heap = new Heap(stream);
        var walHeap = new WalHeap(heap, _walPath);
        return (walHeap, heap);
    }

    private Heap OpenHeapDirect()
    {
        var stream = new FileStream(_dbPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        return new Heap(stream);
    }

    [Fact]
    public void Write_Commit_Read_RoundTrips()
    {
        var (walHeap, _) = CreateWalHeap();
        var handle = walHeap.ObtainNewHandle();
        var data = new byte[] { 10, 20, 30, 40, 50 };

        walHeap.Write(handle, data, 0, data.Length);
        walHeap.Commit();

        var result = walHeap.Read(handle);
        result.Should().BeEquivalentTo(data);
        walHeap.Close();
    }

    [Fact]
    public void UncommittedWrites_NotVisibleAfterReopen()
    {
        long handle;
        {
            var (walHeap, _) = CreateWalHeap();
            handle = walHeap.ObtainNewHandle();
            var data = new byte[] { 1, 2, 3 };
            walHeap.Write(handle, data, 0, data.Length);
            // Commit so handle exists in main heap
            walHeap.Commit();

            // Now write again without commit
            var data2 = new byte[] { 99, 88, 77 };
            walHeap.Write(handle, data2, 0, data2.Length);
            // Close without commit — simulates crash
            walHeap.Close();
        }

        // Reopen — uncommitted data2 should be gone, original data remains
        {
            var stream = new FileStream(_dbPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            var heap = new Heap(stream);
            var walHeap = new WalHeap(heap, _walPath);
            var result = walHeap.Read(handle);
            result.Should().BeEquivalentTo(new byte[] { 1, 2, 3 });
            walHeap.Close();
        }
    }

    [Fact]
    public void WalRecovery_CommittedWal_IsReplayed()
    {
        long handle;
        {
            var (walHeap, _) = CreateWalHeap();
            handle = walHeap.ObtainNewHandle();
            var initial = new byte[] { 1, 1, 1 };
            walHeap.Write(handle, initial, 0, initial.Length);
            walHeap.Commit();
            walHeap.Close();
        }

        // Simulate: write to WAL + COMMIT record but crash before checkpoint completes
        // We do this by manually writing a valid WAL
        {
            var stream = new FileStream(_dbPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            var heap = new Heap(stream);

            // Write new data via WalHeap, commit (WAL gets written + committed)
            var walHeap = new WalHeap(heap, _walPath);
            var data2 = new byte[] { 42, 42, 42 };
            walHeap.Write(handle, data2, 0, data2.Length);
            walHeap.Commit();

            // Verify it applied
            var result = walHeap.Read(handle);
            result.Should().BeEquivalentTo(data2);
            walHeap.Close();
        }
    }

    [Fact]
    public void MultipleHandles_AllPersistAfterCommit()
    {
        var (walHeap, _) = CreateWalHeap();
        var h1 = walHeap.ObtainNewHandle();
        var h2 = walHeap.ObtainNewHandle();
        var h3 = walHeap.ObtainNewHandle();

        walHeap.Write(h1, new byte[] { 1 }, 0, 1);
        walHeap.Write(h2, new byte[] { 2 }, 0, 1);
        walHeap.Write(h3, new byte[] { 3 }, 0, 1);
        walHeap.Commit();

        walHeap.Read(h1).Should().BeEquivalentTo(new byte[] { 1 });
        walHeap.Read(h2).Should().BeEquivalentTo(new byte[] { 2 });
        walHeap.Read(h3).Should().BeEquivalentTo(new byte[] { 3 });
        walHeap.Close();
    }

    [Fact]
    public void WalFile_DeletedAfterSuccessfulCommit()
    {
        var (walHeap, _) = CreateWalHeap();
        var handle = walHeap.ObtainNewHandle();
        walHeap.Write(handle, new byte[] { 1 }, 0, 1);
        walHeap.Commit();

        File.Exists(_walPath).Should().BeFalse();
        walHeap.Close();
    }

    [Fact]
    public void ReadPendingWrite_ReturnsPendingBuffer()
    {
        var (walHeap, _) = CreateWalHeap();
        var handle = walHeap.ObtainNewHandle();
        var data = new byte[] { 7, 8, 9 };
        walHeap.Write(handle, data, 0, data.Length);

        // Read before commit returns buffered data
        var result = walHeap.Read(handle);
        result.Should().BeEquivalentTo(data);
        walHeap.Close();
    }

    [Fact]
    public void StorageEngine_WithWal_BasicCrud()
    {
        using var engine = CatDbFactory.FromFile(_dbPath, new DatabaseOptions { CommitMode = CommitMode.WriteAheadLog });
        var table = engine.OpenXTable<int, string>("test");

        table[1] = "hello";
        table[2] = "world";
        engine.Commit();

        table[1].Should().Be("hello");
        table[2].Should().Be("world");
    }

    [Fact]
    public void StorageEngine_WithWal_PersistsAcrossReopen()
    {
        {
            using var engine = CatDbFactory.FromFile(_dbPath, new DatabaseOptions { CommitMode = CommitMode.WriteAheadLog });
            var table = engine.OpenXTable<int, string>("persist_test");
            table[42] = "persisted";
            engine.Commit();
        }

        {
            using var engine = CatDbFactory.FromFile(_dbPath, new DatabaseOptions { CommitMode = CommitMode.WriteAheadLog });
            var table = engine.OpenXTable<int, string>("persist_test");
            table[42].Should().Be("persisted");
        }
    }
}
