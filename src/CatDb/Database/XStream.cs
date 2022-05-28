using CatDb.General.Extensions;
using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public class XStream : Stream
    {
        internal const int BLOCK_SIZE = 2 * 1024;

        private readonly object _syncRoot;

        private long _position;
        private bool _isModified;
        private long _cachedLength;

        public ITable<IData, IData> Table { get; private set; }

        internal XStream(ITable<IData, IData> table)
        {
            Table = table;
            _syncRoot = new object();
            SetCahchedLenght();
        }

        private void SetCahchedLenght()
        {
            foreach (var row in Table.Backward())
            {
                var key = (Data<long>)row.Key;
                var rec = (Data<byte[]>)row.Value;

                _isModified = false;

                _cachedLength = key.Value + rec.Value.Length;
                break;
            }
        }

        public IDescriptor Description => Table.Descriptor;

        #region Stream Members

        public override void Write(byte[] buffer, int offset, int count)
        {
            lock (_syncRoot)
            {
                while (count > 0)
                {
                    var chunk = Math.Min(BLOCK_SIZE - (int)(_position % BLOCK_SIZE), count);

                    IData key = new Data<long>(_position);
                    IData record = new Data<byte[]>(buffer.Middle(offset, chunk));
                    Table[key] = record;

                    _position += chunk;
                    offset += chunk;
                    count -= chunk;
                }

                _isModified = true;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer");

            if (offset < 0)
                throw new ArgumentException("offset < 0");

            if (count < 0)
                throw new ArgumentException("count < 0");

            if (offset + count > buffer.Length)
                throw new ArgumentException("offset + count > buffer.Length");

            lock (_syncRoot)
            {
                var result = (int)(Length - _position);

                if (result > count)
                    result = count;
                if (result <= 0)
                    return 0;

                var fromKey = new Data<long>(_position - _position % BLOCK_SIZE);
                var toKey = new Data<long>(_position + count - 1);

                long currentKey = -1;

                var sourceOffset = 0;
                var readCount = 0;
                var bufferOffset = offset;

                foreach (var kv in Table.Forward(fromKey, true, toKey, true))
                {
                    var key = ((Data<long>)kv.Key).Value;
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

                    var clearCount = BLOCK_SIZE - (sourceOffset + readCount);
                    var bufferRemainder = (result + offset) - bufferOffset;
                    if (clearCount > bufferRemainder)
                        clearCount = bufferRemainder;

                    Array.Clear(buffer, bufferOffset, clearCount);
                    bufferOffset += clearCount;

                    currentKey = key + BLOCK_SIZE;

                    sourceOffset = 0;
                    readCount = 0;
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
        }

        public override void Flush()
        {
            //do nothing
        }

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => true;

        public override long Length
        {
            get
            {
                lock (_syncRoot)
                {
                    if (!_isModified)
                        return _cachedLength;
                    else
                    {
                        SetCahchedLenght();

                        return _cachedLength;
                    }
                }
            }
        }

        public override long Position
        {
            get
            {
                lock (_syncRoot)
                    return _position;
            }
            set
            {
                lock (_syncRoot)
                    _position = value;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            lock (_syncRoot)
            {
                switch (origin)
                {
                    case SeekOrigin.Begin:
                        _position = offset;
                        break;
                    case SeekOrigin.Current:
                        _position += offset;
                        break;
                    case SeekOrigin.End:
                        _position = Length - 1 - offset;
                        break;
                }

                return _position;
            }
        }

        public override void SetLength(long value)
        {
            lock (_syncRoot)
            {
                var length = Length;
                if (value == length)
                    return;

                var oldPosition = _position;
                try
                {
                    if (value > length)
                    {
                        Seek(value - 1, SeekOrigin.Begin);
                        Write(new byte[1] { 0 }, 0, 1);
                    }
                    else //if (value < length)
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
        }

        #endregion

        public void Zero(long count)
        {
            lock (_syncRoot)
            {
                var fromKey = new Data<long>(_position);
                var toKey = new Data<long>(_position + count - 1);
                Table.Delete(fromKey, toKey);

                _position += count;

                _isModified = true;
            }
        }
    }
}