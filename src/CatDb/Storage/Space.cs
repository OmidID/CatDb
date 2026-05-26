namespace CatDb.Storage;
/// <summary>
/// Strategies for free space allocation.
/// </summary>
public enum AllocationStrategy : byte
{
    /// <summary>
    /// Searches for free space from the current block forwards (default behaviour).
    /// </summary>
    FromTheCurrentBlock,

    /// <summary>
    /// Always searches for free space from the beginning (reduces the space, but may affect the read/write speed).
    /// </summary>
    FromTheBeginning
}

public class Space
{
    private int _activeChunkIndex = -1;
    private readonly List<Ptr> _free = new(); //free chunks are always: ordered by position, not overlapped & not contiguous

    public AllocationStrategy Strategy;

    public long FreeBytes { get; private set; }

    public int FreeChunkCount => _free.Count;

    public Space()
    {
        Strategy = AllocationStrategy.FromTheCurrentBlock;
    }

    public void Add(Ptr freeChunk)
    {
        if (_free.Count == 0)
            _free.Add(freeChunk);
        else
        {
            var last = _free[_free.Count - 1];
            if (freeChunk.Position > last.PositionPlusSize)
                _free.Add(freeChunk);
            else if (freeChunk.Position == last.PositionPlusSize)
            {
                last.Size += freeChunk.Size;
                _free[_free.Count - 1] = last;
            }
            else
                throw new ArgumentException("Invalid ptr order.");
        }

        FreeBytes += freeChunk.Size;
    }

    public Ptr Alloc(long size)
    {
#if PERFORMANCE_CHECK
        var perfStart = System.Diagnostics.Stopwatch.GetTimestamp();
        var scannedChunks = 0;
        var usedWrapAround = false;
#endif

        if (_free.Count == 0)
            throw new Exception("Not enough space.");

        var needSearch = _activeChunkIndex < 0 ||
                         _activeChunkIndex >= _free.Count ||
                         _free[_activeChunkIndex].Size < size;

        if (needSearch)
        {
            var idx = Strategy switch
            {
                AllocationStrategy.FromTheCurrentBlock => _activeChunkIndex >= 0 &&
                                                          _activeChunkIndex + 1 < _free.Count
                    ? _activeChunkIndex + 1
                    : 0,
                AllocationStrategy.FromTheBeginning => 0,
                _ => throw new NotSupportedException(Strategy.ToString())
            };

            var found = false;

            for (var i = idx; i < _free.Count; i++)
            {
#if PERFORMANCE_CHECK
                scannedChunks++;
#endif
                if (_free[i].Size >= size)
                {
                    _activeChunkIndex = i;
                    found = true;
                    break;
                }
            }

            if (!found && Strategy == AllocationStrategy.FromTheCurrentBlock && idx > 0)
            {
                // Wrap search to the beginning to avoid missing reusable chunks.
#if PERFORMANCE_CHECK
                usedWrapAround = true;
#endif
                for (var i = 0; i < idx; i++)
                {
#if PERFORMANCE_CHECK
                    scannedChunks++;
#endif
                    if (_free[i].Size >= size)
                    {
                        _activeChunkIndex = i;
                        found = true;
                        break;
                    }
                }
            }

            if (!found)
                throw new Exception("Not enough space.");
        }

        var ptr = _free[_activeChunkIndex];

        if (ptr.Size < size)
            throw new Exception("Not enough space.");

        var pos = ptr.Position;
        ptr.Position += size;
        ptr.Size -= size;

        if (ptr.Size > 0)
            _free[_activeChunkIndex] = ptr;
        else //if (ptr.Size == 0)
        {
            _free.RemoveAt(_activeChunkIndex);
            _activeChunkIndex = -1; //search for active chunk at next alloc
        }

        FreeBytes -= size;

#if PERFORMANCE_CHECK
        CatDb.General.Diagnostics.PerformanceCheck.Observe("space.alloc.request.bytes", size);
        CatDb.General.Diagnostics.PerformanceCheck.Observe("space.alloc.scan.chunks", scannedChunks);
        CatDb.General.Diagnostics.PerformanceCheck.Observe("space.free.chunks", _free.Count);
        if (usedWrapAround)
            CatDb.General.Diagnostics.PerformanceCheck.Increment("space.alloc.scan.wraparound");
        CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("space.alloc", perfStart);
#endif

        return new Ptr(pos, size);
    }

    public void Free(Ptr ptr)
    {
#if PERFORMANCE_CHECK
        var perfStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif

        var idx = _free.BinarySearch(ptr);
        if (idx >= 0)
            throw new ArgumentException("Space already freed.");

        idx = ~idx;
        if ((idx < _free.Count && ptr.PositionPlusSize > _free[idx].Position) || (idx > 0 && ptr.Position < _free[idx - 1].PositionPlusSize))
            throw new ArgumentException("Can't free overlapped space.");

        var merged = false;

        if (idx < _free.Count) //try merge with right chunk
        {
            var p = _free[idx];
            if (ptr.PositionPlusSize == p.Position)
            {
                p.Position -= ptr.Size;
                p.Size += ptr.Size;
                _free[idx] = p;
                merged = true;
            }
        }

        if (idx > 0) //try merge with left chunk
        {
            var p = _free[idx - 1];
            if (ptr.Position == p.PositionPlusSize)
            {
                if (merged)
                {
                    p.Size += _free[idx].Size;
                    _free[idx - 1] = p;
                    _free.RemoveAt(idx);
                    if (_activeChunkIndex >= idx)
                        _activeChunkIndex--;
                }
                else
                {
                    p.Size += ptr.Size;
                    _free[idx - 1] = p;
                    merged = true;
                }
            }
        }

        if (!merged)
        {
            _free.Insert(idx, ptr);
            if (_activeChunkIndex >= idx)
                _activeChunkIndex++;
        }

        FreeBytes += ptr.Size;

    #if PERFORMANCE_CHECK
        CatDb.General.Diagnostics.PerformanceCheck.Observe("space.free.bytes", ptr.Size);
        CatDb.General.Diagnostics.PerformanceCheck.Observe("space.free.chunks", _free.Count);
        CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("space.free", perfStart);
    #endif
    }

    public void Serialize(BinaryWriter writer)
    {
        writer.Write((byte)Strategy);
        writer.Write(_activeChunkIndex);
        writer.Write(_free.Count);

        for (var i = 0; i < _free.Count; i++)
            _free[i].Serialize(writer);
    }

    public void Deserealize(BinaryReader reader)
    {
        Strategy = (AllocationStrategy)reader.ReadByte();
        _activeChunkIndex = reader.ReadInt32();
        var count = reader.ReadInt32();

        _free.Clear();
        FreeBytes = 0;

        for (var i = 0; i < count; i++)
        {
            var ptr = Ptr.Deserialize(reader);
            _free.Add(ptr);
            FreeBytes += ptr.Size;
        }
    }
}
