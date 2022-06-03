namespace CatDb.General.IO
{
    /// <summary>
    /// An optimized FileStram - optimizes calls to Seek & Size methods
    /// The requirement is if the file is opened for writing, it is an exclusive.
    /// </summary>
    public class OptimizedFileStream : FileStream
    {
        private long _length = long.MinValue;

        public OptimizedFileStream(string path, FileMode mode, FileAccess access, FileShare share, int bufferSize)
            : base(path, mode, access, share, bufferSize)
        {
        }

        public OptimizedFileStream(string fileName, FileMode mode, FileAccess access)
            : base(fileName, mode, access)
        {
        }

        public OptimizedFileStream(string fileName, FileMode mode)
            : base(fileName, mode)
        {
        }

        public override long Position
        {
            get => base.Position;
            set
            {
                if (base.Position != value)
                    base.Position = value;
            }
        }

        public override void Write(byte[] array, int offset, int count)
        {
            try
            {
                base.Write(array, offset, count);

                if (Position > Length)
                    _length = Position;
            }
            catch (Exception)
            {
                _length = long.MinValue;
                throw;
            }
        }

        public override void WriteByte(byte value)
        {
            try
            {
                base.WriteByte(value);

                if (Position > Length)
                    _length = Position;
            }
            catch (Exception)
            {
                _length = long.MinValue;
                throw;
            }
        }

        public override long Length
        {
            get
            {
                if (_length == long.MinValue)
                    _length = base.Length;

                return _length;
            }
        }

        public override void SetLength(long value)
        {
            try
            {
                base.SetLength(value);

                _length = value;
            }
            catch (Exception)
            {
                _length = long.MinValue;

                throw;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    {
                        if (offset != Position)
                            return base.Seek(offset, origin);
                    }
                    break;
                case SeekOrigin.Current:
                    {
                        if (offset != 0)
                            return base.Seek(offset, origin);
                    }
                    break;
                case SeekOrigin.End:
                    {
                        if (offset != Length - Position)
                            return base.Seek(offset, origin);
                    }
                    break;
            }

            return Position;
        }
    }
}
