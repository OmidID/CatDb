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

    /// Read the data stored at the handle.
    byte[] Read(long handle);

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
