// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Storage;
using CatDb.WaterfallTree;
using FluentAssertions;

namespace CatDb.Tests.Storage;

/// <summary>
/// Tests for the Heap storage layer — the low-level handle-based binary store.
/// </summary>
public class HeapTests
{
    private static Heap CreateHeap(bool useCompression = false)
    {
        var ms = new MemoryStream();
        return new Heap(ms, useCompression);
    }

    [Fact]
    public void ObtainHandle_ReturnsUniqueHandles()
    {
        var heap = CreateHeap();
        var h1 = heap.ObtainNewHandle();
        var h2 = heap.ObtainNewHandle();
        h1.Should().NotBe(h2);
    }

    [Fact]
    public void Write_Read_RoundTrips()
    {
        var heap = CreateHeap();
        var handle = heap.ObtainNewHandle();
        var data = new byte[] { 1, 2, 3, 4, 5 };

        heap.Write(handle, data, 0, data.Length);
        heap.Commit();

        var result = heap.Read(handle);
        result.Should().BeEquivalentTo(data);
    }

    [Fact]
    public void Write_LargeBuffer_RoundTrips()
    {
        var heap = CreateHeap();
        var handle = heap.ObtainNewHandle();
        var data = new byte[64 * 1024]; // 64 KB
        new Random(42).NextBytes(data);

        heap.Write(handle, data, 0, data.Length);
        heap.Commit();

        heap.Read(handle).Should().BeEquivalentTo(data);
    }

    [Fact]
    public void Write_MultipleHandles_ReadCorrectData()
    {
        var heap = CreateHeap();
        var h1 = heap.ObtainNewHandle();
        var h2 = heap.ObtainNewHandle();

        heap.Write(h1, new byte[] { 0xAA }, 0, 1);
        heap.Write(h2, new byte[] { 0xBB }, 0, 1);
        heap.Commit();

        heap.Read(h1).Should().BeEquivalentTo(new byte[] { 0xAA });
        heap.Read(h2).Should().BeEquivalentTo(new byte[] { 0xBB });
    }

    [Fact]
    public void Exists_AfterWrite_ReturnsTrue()
    {
        var heap = CreateHeap();
        var handle = heap.ObtainNewHandle();
        heap.Write(handle, new byte[] { 1 }, 0, 1);
        heap.Commit();
        heap.Exists(handle).Should().BeTrue();
    }

    [Fact]
    public void Exists_ReleasedHandle_ReturnsFalse()
    {
        var heap = CreateHeap();
        var handle = heap.ObtainNewHandle();
        heap.Write(handle, new byte[] { 1 }, 0, 1);
        heap.Commit();
        heap.Release(handle);
        heap.Commit();
        heap.Exists(handle).Should().BeFalse();
    }

    [Fact]
    public void Write_WithCompression_RoundTrips()
    {
        var heap = CreateHeap(useCompression: true);
        var handle = heap.ObtainNewHandle();
        // compressible data — repeated bytes
        var data = Enumerable.Repeat((byte)0x42, 4096).ToArray();

        heap.Write(handle, data, 0, data.Length);
        heap.Commit();

        heap.Read(handle).Should().BeEquivalentTo(data);
    }

    [Fact]
    public void Heap_PersistsAcrossReopen()
    {
        var stream = new MemoryStream();
        long handle;
        var written = new byte[] { 10, 20, 30 };

        // write
        var heap = new Heap(stream, false);
        {
            handle = heap.ObtainNewHandle();
            heap.Write(handle, written, 0, written.Length);
            heap.Commit();
        }

        // reopen from same stream
        stream.Seek(0, SeekOrigin.Begin);
        var heap2 = new Heap(stream);
        heap2.Read(handle).Should().BeEquivalentTo(written);
    }
}
