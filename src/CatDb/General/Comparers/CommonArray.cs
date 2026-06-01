// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using System.Runtime.InteropServices;

namespace CatDb.General.Comparers;
[StructLayout(LayoutKind.Explicit)]
public struct CommonArray
{
    [FieldOffset(0)]
    public byte[] ByteArray;

    [FieldOffset(0)]
    public short[] Int16Array;

    [FieldOffset(0)]
    public ushort[] UInt16Array;

    [FieldOffset(0)]
    public int[] Int32Array;

    [FieldOffset(0)]
    public uint[] UInt32Array;

    [FieldOffset(0)]
    public long[] Int64Array;

    [FieldOffset(0)]
    public ulong[] UInt64Array;

    [FieldOffset(0)]
    public float[] SingleArray;

    [FieldOffset(0)]
    public double[] DoubleArray;
}
