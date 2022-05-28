using CatDb.General.IO;
using CatDb.WaterfallTree;
using System.Diagnostics;
using System.IO.Compression;

namespace CatDb.Storage
{
    public class Heap : IHeap
    {
        private readonly object _syncRoot = new();
        private readonly AtomicHeader _header;
        private readonly Space _space;

        //updated every time after Serialize() invocation.
        private long _maxPositionPlusSize;

        //handle -> pointer
        private readonly Dictionary<long, Pointer> _used;
        private readonly Dictionary<long, Pointer> _reserved;

        private long _currentVersion;
        private long _maxHandle;

        public Stream Stream { get; private set; }

        public AllocationStrategy Strategy
        {
            get
            {
                lock (_syncRoot)
                    return _space.Strategy;
            }
            set
            {
                lock (_syncRoot)
                    _space.Strategy = value;
            }
        }

        public Heap(Stream stream, bool useCompression = false, AllocationStrategy strategy = AllocationStrategy.FromTheCurrentBlock)
        {
            stream.Seek(0, SeekOrigin.Begin); //support Seek?

            Stream = stream;

            _space = new Space();

            _used = new Dictionary<long, Pointer>();
            _reserved = new Dictionary<long, Pointer>();

            if (stream.Length < AtomicHeader.SIZE) //create new
            {
                _header = new AtomicHeader
                {
                    UseCompression = useCompression
                };
                _space.Add(new Ptr(AtomicHeader.SIZE, long.MaxValue - AtomicHeader.SIZE));
            }
            else //open exist (ignore the useCompression flag)
            {
                _header = AtomicHeader.Deserialize(Stream);
                stream.Seek(_header.SystemData.Position, SeekOrigin.Begin);
                Deserialize(new BinaryReader(stream));

                //manual alloc header.SystemData
                var ptr = _space.Alloc(_header.SystemData.Size);
                if (ptr.Position != _header.SystemData.Position)
                    throw new Exception("Logical error.");
            }

            Strategy = strategy;

            _currentVersion++;
        }

        public Heap(string fileName, bool useCompression = false, AllocationStrategy strategy = AllocationStrategy.FromTheCurrentBlock)
            :this(new OptimizedFileStream(fileName, FileMode.OpenOrCreate), useCompression, strategy)
        {
        }

        private void FreeOldVersions()
        {
            var forRemove = new List<long>();

            foreach (var kv in _reserved)
            {
                var handle = kv.Key;
                var pointer = kv.Value;
                if (pointer.RefCount > 0)
                    continue;

                _space.Free(pointer.Ptr);
                forRemove.Add(handle);
            }

            foreach (var handle in forRemove)
                _reserved.Remove(handle);
        }

        private void InternalWrite(long position, int originalCount, byte[] buffer, int index, int count)
        {
            var writer = new BinaryWriter(Stream);
            Stream.Seek(position, SeekOrigin.Begin);

            if (UseCompression)
                writer.Write(originalCount);

            writer.Write(buffer, index, count);
        }

        private byte[] InternalRead(long position, long size)
        {
            var reader = new BinaryReader(Stream);
            Stream.Seek(position, SeekOrigin.Begin);

            byte[] buffer;

            if (!UseCompression)
                buffer = reader.ReadBytes((int)size);
            else
            {
                var raw = new byte[reader.ReadInt32()];
                buffer = reader.ReadBytes((int)size - sizeof(int));

                using (var stream = new MemoryStream(buffer))
                {
                    using (var decompress = new DeflateStream(stream, CompressionMode.Decompress))
                        decompress.Read(raw, 0, raw.Length);
                }

                buffer = raw;
            }

            return buffer;
        }

        private void Serialize(BinaryWriter writer)
        {
            _maxPositionPlusSize = AtomicHeader.SIZE;

            writer.Write(_maxHandle);
            writer.Write(_currentVersion);

            //write free
            _space.Serialize(writer);

            //write used
            writer.Write(_used.Count);
            foreach (var kv in _used)
            {
                writer.Write(kv.Key);
                kv.Value.Serialize(writer);

                var posPlusSize = kv.Value.Ptr.PositionPlusSize;
                if (posPlusSize > _maxPositionPlusSize)
                    _maxPositionPlusSize = posPlusSize;
            }

            //write reserved
            writer.Write(_reserved.Count);
            foreach (var kv in _reserved)
            {
                writer.Write(kv.Key);
                kv.Value.Serialize(writer);

                var posPlusSize = kv.Value.Ptr.PositionPlusSize;
                if (posPlusSize > _maxPositionPlusSize)
                    _maxPositionPlusSize = posPlusSize;
            }
        }

        private void Deserialize(BinaryReader reader)
        {
            _maxHandle = reader.ReadInt64();
            _currentVersion = reader.ReadInt64();

            //read free
            _space.Deserealize(reader);

            //read used
            var count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                var handle = reader.ReadInt64();
                var pointer = Pointer.Deserialize(reader);
                _used.Add(handle, pointer);
            }

            //read reserved
            count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                var handle = reader.ReadInt64();
                var pointer = Pointer.Deserialize(reader);
                _reserved.Add(handle, pointer);
            }
        }

        public byte[] Tag
        {
            get
            {
                lock (_syncRoot)
                    return _header.Tag;
            }
            set
            {
                lock (_syncRoot)
                    _header.Tag = value;
            }
        }

        public long ObtainNewHandle()
        {
            lock (_syncRoot)
                return _maxHandle++;
        }

        public void Release(long handle)
        {
            lock (_syncRoot)
            {
                if (!_used.TryGetValue(handle, out var pointer))
                    return; //throw new ArgumentException("handle");

                if (pointer.Version == _currentVersion)
                    _space.Free(pointer.Ptr);
                else
                {
                    pointer.IsReserved = true;
                    _reserved.Add(handle, pointer);
                }

                _used.Remove(handle);
            }
        }

        public bool Exists(long handle)
        {
            lock (_syncRoot)
                return _used.ContainsKey(handle);
        }

        /// <summary>
        /// Before writting, handle must be obtained (registered).
        /// New block will be written always with version = CurrentVersion
        /// If new block is written to handle and the last block of this handle have same version with the new one, occupied space by the last block will be freed.
        /// </summary>
        public void Write(long handle, byte[] buffer, int index, int count)
        {
            var originalCount = count;

            if (UseCompression)
            {
                using var stream = new MemoryStream();
                using (var compress = new DeflateStream(stream, CompressionMode.Compress, true))
                    compress.Write(buffer, index, count);

                buffer = stream.GetBuffer();
                index = 0;
                count = (int)stream.Length;
            }

            lock (_syncRoot)
            {
                if (handle >= _maxHandle)
                    throw new ArgumentException("Invalid handle.");

                if (_used.TryGetValue(handle, out var pointer))
                {
                    if (pointer.Version == _currentVersion)
                        _space.Free(pointer.Ptr);
                    else
                    {
                        pointer.IsReserved = true;
                        _reserved.Add(handle, pointer);
                    }
                }

                long size = UseCompression ? sizeof(int) + count : count;
                var ptr = _space.Alloc(size);
                _used[handle] = pointer = new Pointer(_currentVersion, ptr);

                InternalWrite(ptr.Position, originalCount, buffer, index, count);
            }
        }

        public byte[] Read(long handle)
        {
            lock (_syncRoot)
            {
                if (!_used.TryGetValue(handle, out var pointer))
                    throw new ArgumentException("No such handle or data exists.");

                var ptr = pointer.Ptr;
                Debug.Assert(ptr != Ptr.NULL);

                return InternalRead(ptr.Position, ptr.Size);
            }
        }

        public void Commit()
        {
            lock (_syncRoot)
            {
                Stream.Flush();

                FreeOldVersions();

                using (var ms = new MemoryStream())
                {
                    if (_header.SystemData != Ptr.NULL)
                        _space.Free(_header.SystemData);

                    Serialize(new BinaryWriter(ms));

                    var ptr = _space.Alloc(ms.Length);
                    Stream.Seek(ptr.Position, SeekOrigin.Begin);
                    Stream.Write(ms.GetBuffer(), 0, (int)ms.Length);

                    _header.SystemData = ptr;

                    //atomic write
                    _header.Serialize(Stream);

                    if (ptr.PositionPlusSize > _maxPositionPlusSize)
                        _maxPositionPlusSize = ptr.PositionPlusSize;
                }

                Stream.Flush();

                //try to truncate the stream
                if (Stream.Length > _maxPositionPlusSize)
                    Stream.SetLength(_maxPositionPlusSize);

                _currentVersion++;
            }
        }

        public long DataSize
        {
            get
            {
                lock (_syncRoot)
                    return _used.Sum(kv => kv.Value.Ptr.Size);
            }
        }

        public long Size
        {
            get
            {
                lock (_syncRoot)
                    return Stream.Length;
            }
        }

        public bool UseCompression
        {
            get
            {
                lock (_syncRoot)
                    return _header.UseCompression;
            }
        }

        public void Close()
        {
            lock (_syncRoot)
                Stream.Close();
        }

        public IEnumerable<KeyValuePair<long, byte[]>> GetLatest(long atVersion)
        {
            var list = new List<KeyValuePair<long, Pointer>>();

            lock (_syncRoot)
            {
                foreach (var kv in _used.Union(_reserved))
                {
                    var handle = kv.Key;
                    var pointer = kv.Value;

                    if (pointer.Version >= atVersion && pointer.Version < _currentVersion)
                    {
                        list.Add(new KeyValuePair<long, Pointer>(handle, pointer));
                        pointer.RefCount++;
                    }
                }
            }

            foreach (var kv in list)
            {
                var handle = kv.Key;
                var pointer = kv.Value;

                byte[] buffer;
                lock (_syncRoot)
                {
                    buffer = InternalRead(pointer.Ptr.Position, pointer.Ptr.Size);
                    pointer.RefCount--;
                    if (pointer.IsReserved && pointer.RefCount <= 0)
                    {
                        _space.Free(pointer.Ptr);
                        _reserved.Remove(handle);
                    }
                }

                yield return new KeyValuePair<long, byte[]>(handle, buffer);
            }
        }

        public KeyValuePair<long, Ptr>[] GetUsedSpace()
        {
            lock (_syncRoot)
            {
                var array = new KeyValuePair<long, Ptr>[_used.Count + _reserved.Count];

                var idx = 0;
                foreach (var kv in _used.Union(_reserved))
                    array[idx++] = new KeyValuePair<long, Ptr>(kv.Value.Version, kv.Value.Ptr);

                return array;
            }
        }

        public long CurrentVersion
        {
            get
            {
                lock (_syncRoot)
                    return _currentVersion;
            }
        }
    }
}
