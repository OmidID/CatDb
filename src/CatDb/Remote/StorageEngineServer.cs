using CatDb.Data;
using CatDb.Database;
using CatDb.General.Communication;
using CatDb.Remote.Commands;
using CatDb.WaterfallTree;

namespace CatDb.Remote;

/// <summary>
/// Bridges a <see cref="TcpServer"/> to an <see cref="IStorageEngine"/>.
/// Each inbound packet is processed on a ThreadPool thread (<c>Task.Run</c>)
/// so the async receive loop is never blocked by CPU- or disk-bound work.
/// </summary>
public sealed class StorageEngineServer
{
    private readonly Func<XTablePortable, ICommand, ICommand>[] _tableHandlers;
    private readonly Func<ICommand, ICommand>[]                 _engineHandlers;

    private readonly IStorageEngine _storageEngine;
    private readonly TcpServer      _tcpServer;

    public StorageEngineServer(IStorageEngine storageEngine, TcpServer tcpServer)
    {
        _storageEngine = storageEngine ?? throw new ArgumentNullException(nameof(storageEngine));
        _tcpServer     = tcpServer     ?? throw new ArgumentNullException(nameof(tcpServer));

        // Wire the async packet callback — no worker thread needed
        _tcpServer.PacketReceived = HandlePacketAsync;

        _tableHandlers  = new Func<XTablePortable, ICommand, ICommand>[CommandCode.MAX];
        _tableHandlers[CommandCode.REPLACE]                = Replace;
        _tableHandlers[CommandCode.DELETE]                 = Delete;
        _tableHandlers[CommandCode.DELETE_RANGE]           = DeleteRange;
        _tableHandlers[CommandCode.INSERT_OR_IGNORE]       = InsertOrIgnore;
        _tableHandlers[CommandCode.CLEAR]                  = Clear;
        _tableHandlers[CommandCode.TRY_GET]                = TryGet;
        _tableHandlers[CommandCode.FORWARD]                = Forward;
        _tableHandlers[CommandCode.BACKWARD]               = Backward;
        _tableHandlers[CommandCode.FIND_NEXT]              = FindNext;
        _tableHandlers[CommandCode.FIND_AFTER]             = FindAfter;
        _tableHandlers[CommandCode.FIND_PREV]              = FindPrev;
        _tableHandlers[CommandCode.FIND_BEFORE]            = FindBefore;
        _tableHandlers[CommandCode.FIRST_ROW]              = FirstRow;
        _tableHandlers[CommandCode.LAST_ROW]               = LastRow;
        _tableHandlers[CommandCode.COUNT]                  = Count;
        _tableHandlers[CommandCode.XTABLE_DESCRIPTOR_GET]  = GetXIndexDescriptor;
        _tableHandlers[CommandCode.XTABLE_DESCRIPTOR_SET]  = SetXIndexDescriptor;

        _engineHandlers = new Func<ICommand, ICommand>[CommandCode.MAX];
        _engineHandlers[CommandCode.STORAGE_ENGINE_COMMIT]          = StorageEngineCommit;
        _engineHandlers[CommandCode.STORAGE_ENGINE_GET_ENUMERATOR]  = StorageEngineGetEnumerator;
        _engineHandlers[CommandCode.STORAGE_ENGINE_RENAME]          = StorageEngineRename;
        _engineHandlers[CommandCode.STORAGE_ENGINE_EXISTS]          = StorageEngineExist;
        _engineHandlers[CommandCode.STORAGE_ENGINE_FIND_BY_ID]      = StorageEngineFindById;
        _engineHandlers[CommandCode.STORAGE_ENGINE_FIND_BY_NAME]    = StorageEngineFindByNameCommand;
        _engineHandlers[CommandCode.STORAGE_ENGINE_OPEN_XTABLE]     = StorageEngineOpenXIndex;
        _engineHandlers[CommandCode.STORAGE_ENGINE_OPEN_XFILE]      = StorageEngineOpenXFile;
        _engineHandlers[CommandCode.STORAGE_ENGINE_DELETE]          = StorageEngineDelete;
        _engineHandlers[CommandCode.STORAGE_ENGINE_COUNT]           = StorageEngineCount;
        _engineHandlers[CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE]  = StorageEngineGetCacheSize;
        _engineHandlers[CommandCode.STORAGE_ENGINE_SET_CACHE_SIZE]  = StorageEngineSetCacheSize;
        _engineHandlers[CommandCode.HEAP_OBTAIN_NEW_HANDLE]         = HeapObtainNewHandle;
        _engineHandlers[CommandCode.HEAP_RELEASE_HANDLE]            = HeapReleaseHandle;
        _engineHandlers[CommandCode.HEAP_EXISTS_HANDLE]             = HeapExistsHandle;
        _engineHandlers[CommandCode.HEAP_WRITE]                     = HeapWrite;
        _engineHandlers[CommandCode.HEAP_READ]                      = HeapRead;
        _engineHandlers[CommandCode.HEAP_COMMIT]                    = HeapCommit;
        _engineHandlers[CommandCode.HEAP_CLOSE]                     = HeapClose;
        _engineHandlers[CommandCode.HEAP_GET_TAG]                   = HeapGetTag;
        _engineHandlers[CommandCode.HEAP_SET_TAG]                   = HeapSetTag;
        _engineHandlers[CommandCode.HEAP_DATA_SIZE]                 = HeapDataSize;
        _engineHandlers[CommandCode.HEAP_SIZE]                      = HeapSize;
    }

    // ── Start / Stop ─────────────────────────────────────────────────────────

    public Task StartAsync(CancellationToken ct = default) => _tcpServer.StartAsync(ct);
    public Task StopAsync()                                => _tcpServer.StopAsync();

    /// <summary>Sync wrapper — safe to call from non-async code.</summary>
    public void Start() => _tcpServer.Start();
    public void Stop()  => _tcpServer.Stop();

    public bool IsRunning => _tcpServer.IsRunning;

    // ── Packet handler ───────────────────────────────────────────────────────

    private async Task HandlePacketAsync(ServerConnection conn, Packet packet, CancellationToken ct)
    {
        try
        {
            // ProcessPacket is CPU/disk-bound — offload to thread pool
            var responseMs = await Task.Run(() => ProcessPacket(packet.Request), ct).ConfigureAwait(false);
            await conn.SendResponseAsync(packet.Id, responseMs, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _tcpServer.LogError(ex);
        }
    }

    private MemoryStream ProcessPacket(MemoryStream requestStream)
    {
        var reader     = new BinaryReader(requestStream);
        var msgRequest = Message.Deserialize(reader, id => _storageEngine.Find(id));
        var clientDesc = msgRequest.Description;
        var resultCmds = new CommandCollection(1);

        try
        {
            var commands = msgRequest.Commands;

            if (clientDesc != null) // XTable commands
            {
                var table = (XTablePortable)_storageEngine.OpenXTablePortable(
                    clientDesc.Name ?? "", clientDesc.KeyDataType, clientDesc.RecordDataType);
                table.Descriptor.Tag = clientDesc.Tag;

                for (var i = 0; i < commands.Count - 1; i++)
                    _tableHandlers[commands[i].Code](table, commands[i]);

                var last = _tableHandlers[commands[commands.Count - 1].Code](
                    table, commands[commands.Count - 1]);
                if (last != null) resultCmds.Add(last);

                table.Flush();
            }
            else // StorageEngine commands
            {
                var cmd = commands[commands.Count - 1];
                var result = _engineHandlers[cmd.Code](cmd);
                if (result != null) resultCmds.Add(result);
            }
        }
        catch (Exception e)
        {
            resultCmds.Add(new ExceptionCommand(e.Message));
        }

        var ms     = new MemoryStream();
        var writer = new BinaryWriter(ms);
        var responseDesc = clientDesc ?? new Descriptor(
            -1, "", StructureType.RESERVED, DataType.Boolean, DataType.Boolean,
            null, null, DateTime.Now, DateTime.Now, DateTime.Now, null);

        new Message(responseDesc, resultCmds).Serialize(writer);
        ms.Position = 0;
        return ms;
    }

    #region XTable handlers

    private ICommand Replace(XTablePortable table, ICommand command)
    {
        var cmd = (ReplaceCommand)command;
        table.Replace(cmd.Key, cmd.Record);
        return null!;
    }

    private ICommand Delete(XTablePortable table, ICommand command)
    {
        table.Delete(((DeleteCommand)command).Key);
        return null!;
    }

    private ICommand DeleteRange(XTablePortable table, ICommand command)
    {
        var cmd = (DeleteRangeCommand)command;
        table.Delete(cmd.FromKey, cmd.ToKey);
        return null!;
    }

    private ICommand InsertOrIgnore(XTablePortable table, ICommand command)
    {
        var cmd = (InsertOrIgnoreCommand)command;
        table.InsertOrIgnore(cmd.Key, cmd.Record);
        return null!;
    }

    private ICommand Clear(XTablePortable table, ICommand command)
    {
        table.Clear();
        return null!;
    }

    private ICommand TryGet(XTablePortable table, ICommand command)
    {
        var cmd = (TryGetCommand)command;
        table.TryGet(cmd.Key, out var record);
        return new TryGetCommand(cmd.Key, record);
    }

    private ICommand Forward(XTablePortable table, ICommand command)
    {
        var cmd  = (ForwardCommand)command;
        var list = table.Forward(cmd.FromKey!, cmd.FromKey != null, cmd.ToKey!, cmd.ToKey != null)
                        .Take(cmd.PageCount).ToList();
        return new ForwardCommand(cmd.PageCount, cmd.FromKey, cmd.ToKey, list);
    }

    private ICommand Backward(XTablePortable table, ICommand command)
    {
        var cmd  = (BackwardCommand)command;
        var list = table.Backward(cmd.FromKey!, cmd.FromKey != null, cmd.ToKey!, cmd.ToKey != null)
                        .Take(cmd.PageCount).ToList();
        return new BackwardCommand(cmd.PageCount, cmd.FromKey, cmd.ToKey, list);
    }

    private ICommand FindNext(XTablePortable table, ICommand command)
    {
        var cmd = (FindNextCommand)command;
        return new FindNextCommand(cmd.Key, table.FindNext(cmd.Key));
    }

    private ICommand FindAfter(XTablePortable table, ICommand command)
    {
        var cmd = (FindAfterCommand)command;
        return new FindAfterCommand(cmd.Key, table.FindAfter(cmd.Key));
    }

    private ICommand FindPrev(XTablePortable table, ICommand command)
    {
        var cmd = (FindPrevCommand)command;
        return new FindPrevCommand(cmd.Key, table.FindPrev(cmd.Key));
    }

    private ICommand FindBefore(XTablePortable table, ICommand command)
    {
        var cmd = (FindBeforeCommand)command;
        return new FindBeforeCommand(cmd.Key, table.FindBefore(cmd.Key));
    }

    private ICommand FirstRow(XTablePortable table, ICommand command) =>
        new FirstRowCommand(table.FirstRow);

    private ICommand LastRow(XTablePortable table, ICommand command) =>
        new LastRowCommand(table.LastRow);

    private ICommand GetXIndexDescriptor(XTablePortable table, ICommand command)
    {
        var cmd = (XTableDescriptorGetCommand)command;
        cmd.Descriptor = table.Descriptor;
        return new XTableDescriptorGetCommand(cmd.Descriptor);
    }

    private ICommand SetXIndexDescriptor(XTablePortable table, ICommand command)
    {
        var cmd        = (XTableDescriptorSetCommand)command;
        var descriptor = (Descriptor?)cmd.Descriptor;
        if (descriptor?.Tag != null)
            table.Descriptor.Tag = descriptor.Tag;
        return new XTableDescriptorSetCommand(descriptor);
    }

    private ICommand Count(XTablePortable table, ICommand command) =>
        new CountCommand(table.Count());

    #endregion

    #region StorageEngine handlers

    private ICommand StorageEngineCommit(ICommand command)
    {
        _storageEngine.Commit();
        return new StorageEngineCommitCommand();
    }

    private ICommand StorageEngineGetEnumerator(ICommand command)
    {
        var list = new List<IDescriptor>();
        foreach (var loc in _storageEngine)
            list.Add(new Descriptor(loc.Id, loc.Name!, loc.StructureType,
                loc.KeyDataType, loc.RecordDataType,
                loc.KeyType, loc.RecordType,
                loc.CreateTime, loc.ModifiedTime, loc.AccessTime, loc.Tag));
        return new StorageEngineGetEnumeratorCommand(list);
    }

    private ICommand StorageEngineExist(ICommand command)
    {
        var cmd   = (StorageEngineExistsCommand)command;
        var exist = _storageEngine.Exists(cmd.Name);
        return new StorageEngineExistsCommand(exist, cmd.Name);
    }

    private ICommand StorageEngineFindById(ICommand command)
    {
        var cmd = (StorageEngineFindByIdCommand)command;
        var loc = _storageEngine.Find(cmd.Id);
        return new StorageEngineFindByIdCommand(
            new Descriptor(loc.Id, loc.Name!, loc.StructureType,
                loc.KeyDataType, loc.RecordDataType,
                loc.KeyType, loc.RecordType,
                loc.CreateTime, loc.ModifiedTime, loc.AccessTime, loc.Tag),
            cmd.Id);
    }

    private ICommand StorageEngineFindByNameCommand(ICommand command)
    {
        var cmd = (StorageEngineFindByNameCommand)command;
        cmd.Descriptor = _storageEngine[cmd.Name!];
        return new StorageEngineFindByNameCommand(cmd.Name, cmd.Descriptor);
    }

    private ICommand StorageEngineRename(ICommand command)
    {
        var cmd = (StorageEngineRenameCommand)command;
        _storageEngine.Rename(cmd.Name!, cmd.NewName!);
        return new StorageEngineRenameCommand(cmd.Name, cmd.NewName);
    }

    private ICommand StorageEngineOpenXIndex(ICommand command)
    {
        var cmd = (StorageEngineOpenXIndexCommand)command;
        _storageEngine.OpenXTablePortable(cmd.Name, cmd.KeyType, cmd.RecordType);
        var loc = _storageEngine[cmd.Name];
        return new StorageEngineOpenXIndexCommand(loc.Id);
    }

    private ICommand StorageEngineOpenXFile(ICommand command)
    {
        var cmd = (StorageEngineOpenXFileCommand)command;
        _storageEngine.OpenXFile(cmd.Name!);
        var loc = _storageEngine[cmd.Name!];
        return new StorageEngineOpenXFileCommand(loc.Id);
    }

    private ICommand StorageEngineDelete(ICommand command)
    {
        var cmd = (StorageEngineDeleteCommand)command;
        _storageEngine.Delete(cmd.Name!);
        return new StorageEngineDeleteCommand(cmd.Name);
    }

    private ICommand StorageEngineCount(ICommand command)
    {
        return new StorageEngineCountCommand(_storageEngine.Count);
    }

    private ICommand StorageEngineGetCacheSize(ICommand command)
    {
        return new StorageEngineGetCacheSizeCommand(0);
    }

    private ICommand StorageEngineSetCacheSize(ICommand command)
    {
        return command;
    }

    private ICommand HeapObtainNewHandle(ICommand command) =>
        new HeapObtainNewHandleCommand(_storageEngine.Heap.ObtainNewHandle());

    private ICommand HeapReleaseHandle(ICommand command)
    {
        _storageEngine.Heap.Release(((HeapReleaseHandleCommand)command).Handle);
        return new HeapReleaseHandleCommand(-1);
    }

    private ICommand HeapExistsHandle(ICommand command)
    {
        var cmd    = (HeapExistsHandleCommand)command;
        var exists = _storageEngine.Heap.Exists(cmd.Handle);
        return new HeapExistsHandleCommand(cmd.Handle, exists);
    }

    private ICommand HeapWrite(ICommand command)
    {
        var cmd = (HeapWriteCommand)command;
        _storageEngine.Heap.Write(cmd.Handle, cmd.Buffer!, cmd.Index, cmd.Count);
        return new HeapWriteCommand();
    }

    private ICommand HeapRead(ICommand command)
    {
        var cmd    = (HeapReadCommand)command;
        var buffer = _storageEngine.Heap.Read(cmd.Handle);
        return new HeapReadCommand(cmd.Handle, buffer);
    }

    private ICommand HeapCommit(ICommand command)
    {
        _storageEngine.Heap.Commit();
        return command;
    }

    private ICommand HeapClose(ICommand command)
    {
        _storageEngine.Heap.Close();
        return command;
    }

    private ICommand HeapGetTag(ICommand command) =>
        new HeapGetTagCommand(_storageEngine.Heap.Tag);

    private ICommand HeapSetTag(ICommand command)
    {
        _storageEngine.Heap.Tag = ((HeapSetTagCommand)command).Buffer!;
        return new HeapSetTagCommand();
    }

    private ICommand HeapDataSize(ICommand command) =>
        new HeapDataSizeCommand(_storageEngine.Heap.DataSize);

    private ICommand HeapSize(ICommand command) =>
        new HeapSizeCommand(_storageEngine.Heap.Size);

    #endregion
}


