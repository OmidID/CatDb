// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.WaterfallTree;

/// Provides block-based storage referenced by logical handles.
/// Implementations must offer atomic commit (all-or-nothing) and be thread-safe.
public interface IHeap
{
    /// Register a new handle. The returned handle must always be unique.
    long ObtainNewHandle();

    /// Release the allocated space behind the handle.
    void Release(long handle);

    /// Whether the handle exists in the heap.
    bool Exists(long handle);

    /// Write data at the specified handle.
    void Write(long handle, byte[] buffer, int index, int count);

    /// True if <see cref="Write"/> KEEPS a reference to the caller's buffer past the call (e.g. a WAL pending
    /// queue). When false the caller may safely REUSE/pool the buffer it passed to Write after the call
    /// returns (the heap copied it out), avoiding a large byte[] allocation per store that churns the LOH.
    bool RetainsWrittenBuffer { get; }

    /// Read the data stored at the handle.
    byte[] Read(long handle);

    /// Read into a buffer rented from <paramref name="pool"/> — the caller MUST return it after use. Returns
    /// false when the implementation can't safely lend a pooled buffer (e.g. it would expose a live internal
    /// buffer, like a WAL pending write); the caller then falls back to <see cref="Read"/>. Lets the node-load
    /// path reuse buffers instead of allocating a large byte[] per read that churns the LOH.
    bool TryReadPooled(long handle, System.Buffers.ArrayPool<byte> pool, out byte[] rented, out int length);

    /// Atomically commit ALL changes (all or nothing).
    void Commit();

    /// Close the heap and release all resources.
    void Close();

    /// Small user data written atomically with Commit().
    byte[] Tag { get; set; }

    /// Total size in bytes of the user data.
    long DataSize { get; }

    /// Total size in bytes of the heap.
    long Size { get; }
}
