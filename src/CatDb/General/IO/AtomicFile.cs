// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.General.Comparers;

namespace CatDb.General.IO;
public class AtomicFile
{
    private readonly byte[] _header = new byte[512];
    private readonly CommonArray _commonArray;

    private readonly Stream _stream;
    public string FileName { get; private set; }

    public AtomicFile(string fileName)
    {
        _commonArray.ByteArray = _header;

        _stream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite);
        FileName = fileName;

        if (_stream.Length < _header.Length)
        {
            Pos = _header.Length;
            Size = 0;
            _stream.Write(_header, 0, _header.Length);
        }
        else
            _stream.ReadExactly(_header, 0, _header.Length);
    }

    private long Pos
    {
        get => _commonArray.Int64Array[0];
        set => _commonArray.Int64Array[0] = value;
    }

    public int Size
    {
        get => (int)_commonArray.Int64Array[1];
        private set => _commonArray.Int64Array[1] = value;
    }

    public void Write(byte[] buffer, int index, int count)
    {
        if (Pos - 1 - _header.Length >= count)
            Pos = _header.Length;
        else
            Pos = Pos + Size;

        Size = count;

        _stream.Seek(Pos, SeekOrigin.Begin);
        _stream.Write(buffer, index, count);

        _stream.Seek(0, SeekOrigin.Begin);
        _stream.Write(_header, 0, 2 * sizeof(long)); //HEADER.Length
        _stream.Flush();
    }

    public void Write(byte[] buffer)
    {
        Write(buffer, 0, buffer.Length);
    }

    public byte[] Read()
    {
        _stream.Seek(Pos, SeekOrigin.Begin);

        var buffer = new byte[Size];
        _stream.ReadExactly(buffer, 0, buffer.Length);

        return buffer;
    }

    public void Close()
    {
        if (Pos + Size < _stream.Length)
            _stream.SetLength(Pos + Size);

        _stream.Close();
    }

    public long Length => _stream.Length;
}
