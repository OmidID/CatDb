using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Remote.Commands
{
    public class StorageEngineCommitCommand : ICommand
    {
        public bool IsSynchronous => true;

        public int Code => CommandCode.STORAGE_ENGINE_COMMIT;
    }

    public class StorageEngineGetEnumeratorCommand : ICommand
    {
        public List<IDescriptor> Descriptions;

        public StorageEngineGetEnumeratorCommand()
            : this(null)
        {
        }

        public StorageEngineGetEnumeratorCommand(List<IDescriptor> descriptions)
        {
            Descriptions = descriptions;
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.STORAGE_ENGINE_GET_ENUMERATOR;
    }

    public class StorageEngineRenameCommand : ICommand
    {
        public string Name;
        public string NewName;

        public StorageEngineRenameCommand(string name, string newName)
        {
            Name = name;
            NewName = newName;
        }

        public int Code => CommandCode.STORAGE_ENGINE_RENAME;

        public bool IsSynchronous => true;
    }

    public class StorageEngineExistsCommand : ICommand
    {
        public string Name;
        public bool Exist;

        public StorageEngineExistsCommand(string name)
        {
            Name = name;
        }

        public StorageEngineExistsCommand(bool exist, string name)
        {
            Name = name;
            Exist = exist;
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.STORAGE_ENGINE_EXISTS;
    }

    public class StorageEngineFindByIdCommand : ICommand
    {
        public IDescriptor Descriptor;
        public long Id;

        public StorageEngineFindByIdCommand(IDescriptor descriptor, long id)
        {
            Descriptor = descriptor;
            Id = id;
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.STORAGE_ENGINE_FIND_BY_ID;
    }

    public class StorageEngineFindByNameCommand : ICommand
    {
        public string Name;
        public IDescriptor Descriptor;

        public StorageEngineFindByNameCommand(string name, IDescriptor descriptor)
        {
            Name = name;
            Descriptor = descriptor;
        }

        public StorageEngineFindByNameCommand(IDescriptor descriptor)
            : this(null, descriptor)
        {
        }

        public int Code => CommandCode.STORAGE_ENGINE_FIND_BY_NAME;

        public bool IsSynchronous => true;
    }

    public class StorageEngineOpenXIndexCommand : ICommand
    {
        public long Id;
        public string Name;

        public DataType KeyType;
        public DataType RecordType;

        public DateTime CreateTime;

        public StorageEngineOpenXIndexCommand(long id)
        {
            Id = id;
        }

        public StorageEngineOpenXIndexCommand(string name, DataType keyType, DataType recordType, DateTime createTime)
        {
            Id = -1;
            Name = name;

            KeyType = keyType;
            RecordType = recordType;

            CreateTime = createTime;
        }

        public StorageEngineOpenXIndexCommand(string name, DataType keyType, DataType recordType)
            : this(name, keyType, recordType, new DateTime())
        {
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.STORAGE_ENGINE_OPEN_XTABLE;
    }

    public class StorageEngineOpenXFileCommand : ICommand
    {
        public long Id;
        public string Name;

        public StorageEngineOpenXFileCommand(string name)
        {
            Name = name;
        }

        public StorageEngineOpenXFileCommand(long id)
        {
            Id = id;
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.STORAGE_ENGINE_OPEN_XFILE;
    }

    public class StorageEngineDeleteCommand : ICommand
    {
        public string Name;

        public StorageEngineDeleteCommand(string name)
        {
            Name = name;
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.STORAGE_ENGINE_DELETE;
    }

    public class StorageEngineCountCommand : ICommand
    {
        public int Count;

        public StorageEngineCountCommand()
            : this(0)
        {
        }

        public StorageEngineCountCommand(int count)
        {
            Count = count;
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.STORAGE_ENGINE_COUNT;
    }

    public class StorageEngineDescriptionCommand : ICommand
    {
        public IDescriptor Descriptor;

        public StorageEngineDescriptionCommand(IDescriptor description)
        {
            Descriptor = description;
        }

        public int Code => CommandCode.STORAGE_ENGINE_DESCRIPTOR;

        public bool IsSynchronous => true;
    }

    public class StorageEngineGetCacheSizeCommand : ICommand
    {
        public int CacheSize;

        public StorageEngineGetCacheSizeCommand(int cacheSize)
        {
            CacheSize = cacheSize;
        }

        public int Code => CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE;

        public bool IsSynchronous => true;
    }

    public class StorageEngineSetCacheSizeCommand : ICommand
    {
        public int CacheSize;

        public StorageEngineSetCacheSizeCommand(int cacheSize)
        {
            CacheSize = cacheSize;
        }

        public int Code => CommandCode.STORAGE_ENGINE_SET_CACHE_SIZE;

        public bool IsSynchronous => true;
    }

    public class ExceptionCommand : ICommand
    {
        public readonly string Exception;

        public ExceptionCommand(string exception)
        {
            Exception = exception;
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.EXCEPTION;
    }
}