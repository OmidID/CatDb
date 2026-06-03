// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;
using CatDb.General.Extensions;
using CatDb.General.Threading;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class XStream : Stream
{
    internal const int BLOCK_SIZE = 2 * 1024;

    private readonly ReentrantLock _syncRoot = new();
    private long _position;
    private bool _isModified;
    private long _cachedLength;

    public ITable<IData, IData> Table { get; private set; }

    internal XStream(ITable<IData, IData> table)
    {
        Table       = table;
        SetCahchedLenght();
    }

    private void SetCahchedLenght()
    {
        foreach (var row in Table.Backward())
        {
            var key = (Data<long>)row.Key;
            var rec = (Data<byte[]>)row.Value;

            _isModified   = false;
            _cachedLength = key.Value + rec.Value.Length;
            break;
        }
    }

    public IDescriptor Description => Table.Descriptor;

    public override void Write(byte[] buffer, int offset, int count)
    {
        _syncRoot.Enter();
        try
        {
            while (count > 0)
            {
                var chunk = Math.Min(BLOCK_SIZE - (int)(_position % BLOCK_SIZE), count);

                IData key    = new Data<long>(_position);
                IData record = new Data<byte[]>(buffer.Middle(offset, chunk));
                Table[key] = record;

                _position += chunk;
                offset    += chunk;
                count     -= chunk;
            }

            _isModified = true;
        }
        finally { _syncRoot.Exit(); }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        if (offset < 0) throw new ArgumentException("offset < 0");
        if (count  < 0) throw new ArgumentException("count < 0");
        if (offset + count > buffer.Length) throw new ArgumentException("offset + count > buffer.Length");

        _syncRoot.Enter();
        try
        {
            var result = (int)(Length - _position);
            if (result > count) result = count;
            if (result <= 0)    return 0;

            var fromKey = new Data<long>(_position - _position % BLOCK_SIZE);
            var toKey   = new Data<long>(_position + count - 1);

            long currentKey   = -1;
            var  sourceOffset = 0;
            var  readCount    = 0;
            var  bufferOffset = offset;

            foreach (var kv in Table.Forward(fromKey, true, toKey, true))
            {
                var key    = ((Data<long>)kv.Key).Value;
                var source = ((Data<byte[]>)kv.Value).Value;

                if (currentKey < 0)
                {
                    if (_position >= key)
                        sourceOffset = (int)(_position - key);
                    else
                    {
                        Array.Clear(buffer, bufferOffset, (int)(key - _position));
                        bufferOffset += (int)(key - _position);
                    }
                }
                else if (currentKey != key)
                {
                    var difference = (int)(key - currentKey);
                    Array.Clear(buffer, bufferOffset, difference);
                    bufferOffset += difference;
                }

                if (sourceOffset < source.Length)
                    readCount = source.Length - sourceOffset;

                if (bufferOffset + readCount > buffer.Length)
                    readCount = buffer.Length - bufferOffset;

                if (readCount > 0)
                    Buffer.BlockCopy(source, sourceOffset, buffer, bufferOffset, readCount);
                bufferOffset += readCount;

                var clearCount      = BLOCK_SIZE - (sourceOffset + readCount);
                var bufferRemainder = (result + offset) - bufferOffset;
                if (clearCount > bufferRemainder) clearCount = bufferRemainder;

                Array.Clear(buffer, bufferOffset, clearCount);
                bufferOffset += clearCount;

                currentKey   = key + BLOCK_SIZE;
                sourceOffset = 0;
                readCount    = 0;
            }

            if (bufferOffset < result + offset)
            {
                var clearCount = result;
                if (bufferOffset + clearCount > buffer.Length)
                    clearCount = buffer.Length - bufferOffset;
                Array.Clear(buffer, bufferOffset, clearCount);
            }

            _position += result;
            return result;
        }
        finally { _syncRoot.Exit(); }
    }

    public override void Flush() { } // no-op

    public override bool CanRead  => true;
    public override bool CanSeek  => true;
    public override bool CanWrite => true;

    public override long Length
    {
        get
        {
            _syncRoot.Enter();
            try
            {
                if (!_isModified) return _cachedLength;
                SetCahchedLenght();
                return _cachedLength;
            }
            finally { _syncRoot.Exit(); }
        }
    }

    public override long Position
    {
        get
        {
            _syncRoot.Enter();
            try { return _position; }
            finally { _syncRoot.Exit(); }
        }
        set
        {
            _syncRoot.Enter();
            try { _position = value; }
            finally { _syncRoot.Exit(); }
        }
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        _syncRoot.Enter();
        try
        {
            _position = origin switch
            {
                SeekOrigin.Begin   => offset,
                SeekOrigin.Current => _position + offset,
                SeekOrigin.End     => Length - 1 - offset,
                _                  => _position
            };
            return _position;
        }
        finally { _syncRoot.Exit(); }
    }

    public override void SetLength(long value)
    {
        _syncRoot.Enter();
        try
        {
            var length = Length;
            if (value == length) return;

            var oldPosition = _position;
            try
            {
                if (value > length)
                {
                    Seek(value - 1, SeekOrigin.Begin);
                    Write([0], 0, 1);
                }
                else
                {
                    Seek(value, SeekOrigin.Begin);
                    Zero(length - value);
                }
            }
            finally
            {
                Seek(oldPosition, SeekOrigin.Begin);
                _isModified = true;
            }
        }
        finally { _syncRoot.Exit(); }
    }

    public void Zero(long count)
    {
        _syncRoot.Enter();
        try
        {
            var fromKey = new Data<long>(_position);
            var toKey   = new Data<long>(_position + count - 1);
            Table.Delete(fromKey, toKey);

            _position   += count;
            _isModified  = true;
        }
        finally { _syncRoot.Exit(); }
    }
}
