namespace CatDb.Remote.Commands
{
    public class HeapObtainNewHandleCommand : ICommand
    {
        public long Handle;

        public HeapObtainNewHandleCommand(long handle)
        {
            Handle = handle;
        }

        public HeapObtainNewHandleCommand()
        {
        }

        public int Code => CommandCode.HEAP_OBTAIN_NEW_HANDLE;

        public bool IsSynchronous => true;
    }

    public class HeapReleaseHandleCommand : ICommand
    {
        public long Handle;

        public HeapReleaseHandleCommand(long handle)
        {
            Handle = handle;
        }

        public int Code => CommandCode.HEAP_RELEASE_HANDLE;

        public bool IsSynchronous => true;
    }

    public class HeapExistsHandleCommand : ICommand
    {
        public long Handle;
        public bool Exist;

        public HeapExistsHandleCommand(long handle, bool exist)
        {
            Handle = handle;
            Exist = exist;
        }

        public HeapExistsHandleCommand()
        {
        }

        public int Code => CommandCode.HEAP_EXISTS_HANDLE;

        public bool IsSynchronous => true;
    }

    public class HeapWriteCommand : ICommand
    {
        public long Handle;

        public byte[] Buffer;
        public int Index;
        public int Count;

        public HeapWriteCommand(long handle, byte[] buffer, int index, int count)
        {
            Handle = handle;
            Buffer = buffer;

            Index = index;
            Count = count;
        }

        public HeapWriteCommand()
        {
        }

        public int Code => CommandCode.HEAP_WRITE;

        public bool IsSynchronous => true;
    }

    public class HeapReadCommand : ICommand
    {
        public long Handle;
        public byte[] Buffer;

        public HeapReadCommand(long handle, byte[] buffer)
        {
            Handle = handle;
            Buffer = buffer;
        }

        public int Code => CommandCode.HEAP_READ;

        public bool IsSynchronous => true;
    }

    public class HeapCommitCommand : ICommand
    {
        public int Code => CommandCode.HEAP_COMMIT;

        public bool IsSynchronous => true;
    }

    public class HeapCloseCommand : ICommand
    {
        public int Code => CommandCode.HEAP_CLOSE;

        public bool IsSynchronous => true;
    }

    public class HeapGetTagCommand : ICommand
    {
        public byte[] Tag;

        public HeapGetTagCommand(byte[] tag)
        {
            Tag = tag;
        }

        public HeapGetTagCommand()
        {
        }

        public int Code => CommandCode.HEAP_GET_TAG;

        public bool IsSynchronous => true;
    }

    public class HeapSetTagCommand : ICommand
    {
        public byte[] Buffer;

        public HeapSetTagCommand(byte[] buffer)
        {
            Buffer = buffer;
        }

        public HeapSetTagCommand()
        {
        }

        public int Code => CommandCode.HEAP_SET_TAG;

        public bool IsSynchronous => true;
    }

    public class HeapDataSizeCommand : ICommand
    {
        public long DataSize;

        public HeapDataSizeCommand(long dataSize)
        {
            DataSize = dataSize;
        }

        public HeapDataSizeCommand()
        {
        }

        public int Code => CommandCode.HEAP_DATA_SIZE;

        public bool IsSynchronous => true;
    }

    public class HeapSizeCommand : ICommand
    {
        public long Size;

        public HeapSizeCommand(long size)
        {
            Size = size;
        }

        public HeapSizeCommand()
        {
        }

        public int Code => CommandCode.HEAP_SIZE;

        public bool IsSynchronous => true;
    }
}
