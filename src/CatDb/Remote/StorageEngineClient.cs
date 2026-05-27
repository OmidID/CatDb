using System.Collections;
using System.Collections.Concurrent;
using CatDb.Data;
using CatDb.Database;
using CatDb.General.Communication;
using CatDb.Remote.Commands;
using CatDb.WaterfallTree;

namespace CatDb.Remote;

/// <summary>
/// Remote <see cref="IStorageEngine"/> client backed by an async TCP connection.
/// <para>
/// <b>Async-first:</b> use <see cref="ConnectAsync"/> + <see cref="ExecuteAsync"/> for
/// fully non-blocking operation from async callers.
/// </para>
/// <para>
/// <b>Sync compat:</b> the default constructor connects synchronously via
/// <c>Task.Run(…).GetAwaiter().GetResult()</c> which is deadlock-safe because
/// <c>Task.Run</c> strips any <see cref="System.Threading.SynchronizationContext"/>.
/// </para>
/// </summary>
public sealed class StorageEngineClient : IStorageEngine, IAsyncDisposable
{
    private readonly string _databaseName;
    private readonly string? _userName;
    private readonly string? _password;
    private int _cacheSize;
    private readonly ConcurrentDictionary<string, XTableRemote> _indexes = new();
    private readonly Dictionary<TransformerCacheKey, object> _transformerCache = new();

    private static readonly Descriptor StorageEngineDescriptor =
        new(-1, "", DataType.Boolean, DataType.Boolean);

    private readonly ClientConnection _connection;

    private readonly record struct TransformerCacheKey(Type ObjectType, Type DataType);

    // ── Construction ──────────────────────────────────────────────────────────

    /// <summary>
    /// Creates the client and connects synchronously.
    /// Safe to call from any context — uses <c>Task.Run</c> to avoid
    /// SynchronizationContext deadlocks.
    /// </summary>
    public StorageEngineClient(string host = "localhost", int port = 7182, string databaseName = "default", string? userName = null, string? password = null)
    {
        _databaseName = databaseName;
        _userName = userName;
        _password = password;
        _connection = new ClientConnection(host, port);
        // Safe sync connect: Task.Run strips SynchronizationContext
        Task.Run(() => _connection.StartAsync(CancellationToken.None)).GetAwaiter().GetResult();
        Heap = new RemoteHeap(this);
    }

    /// <summary>
    /// Creates the client without connecting.
    /// Call <see cref="ConnectAsync"/> before using.
    /// </summary>
    public static StorageEngineClient CreateUnconnected(string host = "localhost", int port = 7182, string databaseName = "default", string? userName = null, string? password = null)
    {
        var c = new StorageEngineClient(host, port, deferConnect: true, databaseName, userName, password);
        return c;
    }

    private StorageEngineClient(string host, int port, bool deferConnect, string databaseName, string? userName, string? password)
    {
        _databaseName = databaseName;
        _userName = userName;
        _password = password;
        _connection = new ClientConnection(host, port);
        Heap = new RemoteHeap(this);
    }

    /// <summary>Fully async connect — use with <see cref="CreateUnconnected"/>.</summary>
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        await _connection.StartAsync(ct).ConfigureAwait(false);
    }

    // ── Core async execute ────────────────────────────────────────────────────

    /// <summary>Sends a command collection and returns the server's response, fully async.</summary>
    public async Task<CommandCollection> ExecuteAsync(
        IDescriptor descriptor,
        CommandCollection commands,
        CancellationToken ct = default)
    {
        var ms = new MemoryStream();
        new Message(descriptor, commands, _databaseName, _userName, _password).Serialize(new BinaryWriter(ms));
        ms.Position = 0;

        var responseMs = await _connection.SendAsync(new Packet(ms), ct).ConfigureAwait(false);

        var message = Message.Deserialize(new BinaryReader(responseMs), (_, _, _, _) => descriptor);
        return message.Commands;
    }

    /// <summary>
    /// Sync bridge — internally calls <see cref="ExecuteAsync"/> via
    /// <c>Task.Run</c> to avoid SynchronizationContext deadlocks.
    /// </summary>
    public CommandCollection Execute(IDescriptor descriptor, CommandCollection commands) =>
        Task.Run(() => ExecuteAsync(descriptor, commands)).GetAwaiter().GetResult();

    // ── IStorageEngine ────────────────────────────────────────────────────────

    public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(
        string name, DataType keyDataType, DataType recordDataType,
        ITransformer<TKey, IData> keyTransformer, ITransformer<TRecord, IData> recordTransformer)
    {
        var index = OpenXTablePortable(name, keyDataType, recordDataType);
        keyTransformer ??= GetOrCreateTransformer<TKey>(index.Descriptor.KeyType!);
        recordTransformer ??= GetOrCreateTransformer<TRecord>(index.Descriptor.RecordType!);
        return new XTablePortable<TKey, TRecord>(index, keyTransformer, recordTransformer);
    }

    private ITransformer<T, IData> GetOrCreateTransformer<T>(Type dataType)
    {
        var key = new TransformerCacheKey(typeof(T), dataType);
        lock (_transformerCache)
        {
            if (_transformerCache.TryGetValue(key, out var cached))
                return (ITransformer<T, IData>)cached;

            var transformer = new DataTransformer<T>(dataType);
            _transformerCache[key] = transformer;
            return transformer;
        }
    }

    public ITable<IData, IData> OpenXTablePortable(string name, DataType keyType, DataType recordType)
    {
        var cmd = new StorageEngineOpenXIndexCommand(name, keyType, recordType);
        InternalExecute(cmd);
        var descriptor = new Descriptor(cmd.Id, name, keyType, recordType);
        var index = new XTableRemote(this, descriptor);
        _indexes.TryAdd(name, index);
        return index;
    }

    public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name)
    {
        var keyDataType    = DataTypeUtils.BuildDataType(typeof(TKey));
        var recordDataType = DataTypeUtils.BuildDataType(typeof(TRecord));
        return OpenXTablePortable<TKey, TRecord>(name, keyDataType, recordDataType, null!, null!);
    }

    public ITable<TKey, TRecord> OpenXTable<TKey, TRecord>(string name) =>
        OpenXTablePortable<TKey, TRecord>(name);

    public XFile OpenXFile(string name) => throw new NotSupportedException();

    public void Rename(string name, string newName) =>
        InternalExecute(new StorageEngineRenameCommand(name, newName));

    public IDescriptor this[string name] => _indexes[name].Descriptor;

    public void Delete(string name) =>
        InternalExecute(new StorageEngineDeleteCommand(name));

    public bool Exists(string name)
    {
        var cmd = new StorageEngineExistsCommand(name);
        InternalExecute(cmd);
        return cmd.Exist;
    }

    public int Count
    {
        get
        {
            var cmd = new StorageEngineCountCommand();
            InternalExecute(cmd);
            return cmd.Count;
        }
    }

    public IDescriptor Find(long id)
    {
        var cmd = new StorageEngineFindByIdCommand(null, id);
        InternalExecute(cmd);
        return cmd.Descriptor!;
    }

    public IEnumerator<IDescriptor> GetEnumerator()
    {
        var cmd = new StorageEngineGetEnumeratorCommand();
        InternalExecute(cmd);
        return cmd.Descriptions!.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void Commit()
    {
        foreach (var index in _indexes.Values)
            index.Flush();
        InternalExecute(new StorageEngineCommitCommand());
    }

    public IHeap Heap { get; private set; }

    public int CacheSize
    {
        get
        {
            var cmd = new StorageEngineGetCacheSizeCommand(0);
            var col = new CommandCollection(1) { cmd };
            var result = (StorageEngineGetCacheSizeCommand)Execute(StorageEngineDescriptor, col)[0];
            return result.CacheSize;
        }
        set
        {
            _cacheSize = value;
            Execute(StorageEngineDescriptor, new CommandCollection(1) { new StorageEngineSetCacheSizeCommand(_cacheSize) });
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private void InternalExecute(ICommand command)
    {
        var cmds   = new CommandCollection(1) { command };
        var result = Execute(StorageEngineDescriptor, cmds)[0];
        SetResult(command, result);
    }

    private static void SetResult(ICommand command, ICommand result)
    {
        switch (result.Code)
        {
            case CommandCode.STORAGE_ENGINE_COMMIT:         break;
            case CommandCode.STORAGE_ENGINE_OPEN_XTABLE:
                ((StorageEngineOpenXIndexCommand)command).Id         = ((StorageEngineOpenXIndexCommand)result).Id;
                ((StorageEngineOpenXIndexCommand)command).CreateTime = ((StorageEngineOpenXIndexCommand)result).CreateTime;
                break;
            case CommandCode.STORAGE_ENGINE_OPEN_XFILE:
                ((StorageEngineOpenXFileCommand)command).Id = ((StorageEngineOpenXFileCommand)result).Id;
                break;
            case CommandCode.STORAGE_ENGINE_EXISTS:
                ((StorageEngineExistsCommand)command).Exist = ((StorageEngineExistsCommand)result).Exist;
                break;
            case CommandCode.STORAGE_ENGINE_FIND_BY_ID:
                ((StorageEngineFindByIdCommand)command).Descriptor = ((StorageEngineFindByIdCommand)result).Descriptor;
                break;
            case CommandCode.STORAGE_ENGINE_FIND_BY_NAME:
                ((StorageEngineFindByNameCommand)command).Descriptor = ((StorageEngineFindByNameCommand)result).Descriptor;
                break;
            case CommandCode.STORAGE_ENGINE_DELETE:         break;
            case CommandCode.STORAGE_ENGINE_COUNT:
                ((StorageEngineCountCommand)command).Count = ((StorageEngineCountCommand)result).Count;
                break;
            case CommandCode.STORAGE_ENGINE_GET_ENUMERATOR:
                ((StorageEngineGetEnumeratorCommand)command).Descriptions = ((StorageEngineGetEnumeratorCommand)result).Descriptions;
                break;
            case CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE:
                ((StorageEngineGetCacheSizeCommand)command).CacheSize = ((StorageEngineGetCacheSizeCommand)result).CacheSize;
                break;
            case CommandCode.EXCEPTION:
                throw new Exception(((ExceptionCommand)result).Exception);
        }
    }

    // ── Dispose ───────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        await _connection.StopAsync().ConfigureAwait(false);
    }

    public void Dispose() => _connection.StopAsync().GetAwaiter().GetResult();
    public void Close()   => Dispose();

    // ── Remote heap ───────────────────────────────────────────────────────────

    private sealed class RemoteHeap : IHeap
    {
        private readonly StorageEngineClient _engine;

        public RemoteHeap(StorageEngineClient engine) =>
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));

        private void InternalExecute(ICommand command)
        {
            var cmds   = new CommandCollection(1) { command };
            var result = _engine.Execute(StorageEngineDescriptor, cmds)[0];
            SetResult(command, result);
        }

        public long ObtainNewHandle()
        {
            var cmd = new HeapObtainNewHandleCommand();
            InternalExecute(cmd);
            return cmd.Handle;
        }

        public void Release(long handle) =>
            InternalExecute(new HeapReleaseHandleCommand(handle));

        public bool Exists(long handle)
        {
            var cmd = new HeapExistsHandleCommand(handle, false);
            InternalExecute(cmd);
            return cmd.Exist;
        }

        public void Write(long handle, byte[] buffer, int index, int count) =>
            InternalExecute(new HeapWriteCommand(handle, buffer, index, count));

        public byte[] Read(long handle)
        {
            var cmd = new HeapReadCommand(handle, null);
            InternalExecute(cmd);
            return cmd.Buffer!;
        }

        public void Commit() => InternalExecute(new HeapCommitCommand());
        public void Close()  => InternalExecute(new HeapCloseCommand());

        public byte[] Tag
        {
            get
            {
                var cmd = new HeapGetTagCommand();
                InternalExecute(cmd);
                return cmd.Tag!;
            }
            set => InternalExecute(new HeapSetTagCommand(value));
        }

        public long DataSize
        {
            get
            {
                var cmd = new HeapDataSizeCommand();
                InternalExecute(cmd);
                return cmd.DataSize;
            }
        }

        public long Size
        {
            get
            {
                var cmd = new HeapSizeCommand();
                InternalExecute(cmd);
                return cmd.Size;
            }
        }

        private static void SetResult(ICommand command, ICommand result)
        {
            switch (result.Code)
            {
                case CommandCode.HEAP_OBTAIN_NEW_HANDLE:
                    ((HeapObtainNewHandleCommand)command).Handle = ((HeapObtainNewHandleCommand)result).Handle;
                    break;
                case CommandCode.HEAP_RELEASE_HANDLE:   break;
                case CommandCode.HEAP_EXISTS_HANDLE:
                    ((HeapExistsHandleCommand)command).Exist = ((HeapExistsHandleCommand)result).Exist;
                    break;
                case CommandCode.HEAP_WRITE:            break;
                case CommandCode.HEAP_READ:
                    ((HeapReadCommand)command).Buffer = ((HeapReadCommand)result).Buffer;
                    break;
                case CommandCode.HEAP_COMMIT:           break;
                case CommandCode.HEAP_CLOSE:            break;
                case CommandCode.HEAP_SET_TAG:          break;
                case CommandCode.HEAP_GET_TAG:
                    ((HeapGetTagCommand)command).Tag = ((HeapGetTagCommand)result).Tag;
                    break;
                case CommandCode.HEAP_DATA_SIZE:
                    ((HeapDataSizeCommand)command).DataSize = ((HeapDataSizeCommand)result).DataSize;
                    break;
                case CommandCode.HEAP_SIZE:
                    ((HeapSizeCommand)command).Size = ((HeapSizeCommand)result).Size;
                    break;
                case CommandCode.EXCEPTION:
                    throw new Exception(((ExceptionCommand)result).Exception);
            }
        }
    }
}


