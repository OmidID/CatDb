using CatDb.Data;
using CatDb.Database;
using CatDb.WaterfallTree;
using CatDb.General.Communication;
using CatDb.Remote.Commands;

namespace CatDb.Remote
{
    public class StorageEngineServer
    {
        private CancellationTokenSource _shutdownTokenSource;
        private Thread _worker;

        private readonly Func<XTablePortable, ICommand, ICommand>[] _commandsIIndexExecute;
        private readonly Func<ICommand, ICommand>[] _commandsStorageEngineExecute;
        private Func<ICommand, ICommand>[] _commandsHeapExecute;

        private readonly IStorageEngine _storageEngine;
        private readonly TcpServer _tcpServer;

        public StorageEngineServer(IStorageEngine storageEngine, TcpServer tcpServer)
        {
            if (storageEngine == null)
                throw new ArgumentNullException("storageEngine");
            if (tcpServer == null)
                throw new ArgumentNullException("tcpServer");

            _storageEngine = storageEngine;
            _tcpServer = tcpServer;

            _commandsIIndexExecute = new Func<XTablePortable, ICommand, ICommand>[CommandCode.MAX];
            _commandsIIndexExecute[CommandCode.REPLACE] = Replace;
            _commandsIIndexExecute[CommandCode.DELETE] = Delete;
            _commandsIIndexExecute[CommandCode.DELETE_RANGE] = DeleteRange;
            _commandsIIndexExecute[CommandCode.INSERT_OR_IGNORE] = InsertOrIgnore;
            _commandsIIndexExecute[CommandCode.CLEAR] = Clear;
            _commandsIIndexExecute[CommandCode.TRY_GET] = TryGet;
            _commandsIIndexExecute[CommandCode.FORWARD] = Forward;
            _commandsIIndexExecute[CommandCode.BACKWARD] = Backward;
            _commandsIIndexExecute[CommandCode.FIND_NEXT] = FindNext;
            _commandsIIndexExecute[CommandCode.FIND_AFTER] = FindAfter;
            _commandsIIndexExecute[CommandCode.FIND_PREV] = FindPrev;
            _commandsIIndexExecute[CommandCode.FIND_BEFORE] = FindBefore;
            _commandsIIndexExecute[CommandCode.FIRST_ROW] = FirstRow;
            _commandsIIndexExecute[CommandCode.LAST_ROW] = LastRow;
            _commandsIIndexExecute[CommandCode.COUNT] = Count;
            _commandsIIndexExecute[CommandCode.XTABLE_DESCRIPTOR_GET] = GetXIndexDescriptor;
            _commandsIIndexExecute[CommandCode.XTABLE_DESCRIPTOR_SET] = SetXIndexDescriptor;

            _commandsStorageEngineExecute = new Func<ICommand, ICommand>[CommandCode.MAX];
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_COMMIT] = StorageEngineCommit;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_GET_ENUMERATOR] = StorageEngineGetEnumerator;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_RENAME] = StorageEngineRename;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_EXISTS] = StorageEngineExist;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_FIND_BY_ID] = StorageEngineFindById;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_FIND_BY_NAME] = StorageEngineFindByNameCommand;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_OPEN_XTABLE] = StorageEngineOpenXIndex;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_OPEN_XFILE] = StorageEngineOpenXFile;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_DELETE] = StorageEngineDelete;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_COUNT] = StorageEngineCount;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE] = StorageEngineGetCacheSize;
            _commandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_SET_CACHE_SIZE] = StorageEngineSetCacheSize;
            _commandsStorageEngineExecute[CommandCode.HEAP_OBTAIN_NEW_HANDLE] = HeapObtainNewHandle;
            _commandsStorageEngineExecute[CommandCode.HEAP_RELEASE_HANDLE] = HeapReleaseHandle;
            _commandsStorageEngineExecute[CommandCode.HEAP_EXISTS_HANDLE] = HeapExistsHandle;
            _commandsStorageEngineExecute[CommandCode.HEAP_WRITE] = HeapWrite;
            _commandsStorageEngineExecute[CommandCode.HEAP_READ] = HeapRead;
            _commandsStorageEngineExecute[CommandCode.HEAP_COMMIT] = HeapCommit;
            _commandsStorageEngineExecute[CommandCode.HEAP_CLOSE] = HeapClose;
            _commandsStorageEngineExecute[CommandCode.HEAP_GET_TAG] = HeapGetTag;
            _commandsStorageEngineExecute[CommandCode.HEAP_SET_TAG] = HeapSetTag;
            _commandsStorageEngineExecute[CommandCode.HEAP_DATA_SIZE] = HeapDataSize;
            _commandsStorageEngineExecute[CommandCode.HEAP_SIZE] = HeapSize;
        }

        public void Start()
        {
            Stop();

            _shutdownTokenSource = new CancellationTokenSource();

            _worker = new Thread(DoWork);
            _worker.Start();
        }

        public void Stop()
        {
            if (!IsWorking)
                return;

            _shutdownTokenSource.Cancel(false);

            var thread = _worker;
            if (thread != null)
            {
                if (!thread.Join(5000))
                    thread.Abort();
            }
        }

        public bool IsWorking => _worker != null;

        private void DoWork()
        {
            try
            {
                _tcpServer.Start();

                while (!_shutdownTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        var order = _tcpServer.RecievedPackets.Take(_shutdownTokenSource.Token);
                        Task.Factory.StartNew(PacketExecute, order);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception exc)
                    {
                        _tcpServer.LogError(exc);
                    }
                }
            }
            catch (Exception exc)
            {
                _tcpServer.LogError(exc);
            }
            finally
            {
                _tcpServer.Stop();

                _worker = null;
            }
        }

        private void PacketExecute(object state)
        {
            try
            {
                var order = (KeyValuePair<ServerConnection, Packet>)state;

                var reader = new BinaryReader(order.Value.Request);
                var msgRequest = Message.Deserialize(reader, (id) => _storageEngine.Find(id));

                var clientDescription = msgRequest.Description;
                var resultCommands = new CommandCollection(1);

                try
                {
                    var commands = msgRequest.Commands;

                    if (msgRequest.Description != null) // XTable commands
                    {
                        var table = (XTablePortable)_storageEngine.OpenXTablePortable(clientDescription.Name, clientDescription.KeyDataType, clientDescription.RecordDataType);
                        table.Descriptor.Tag = clientDescription.Tag;

                        for (var i = 0; i < commands.Count - 1; i++)
                        {
                            var command = msgRequest.Commands[i];
                            _commandsIIndexExecute[command.Code](table, command);
                        }

                        var resultCommand = _commandsIIndexExecute[msgRequest.Commands[commands.Count - 1].Code](table, msgRequest.Commands[commands.Count - 1]);
                        if (resultCommand != null)
                            resultCommands.Add(resultCommand);

                        table.Flush();
                    }
                    else //Storage engine commands
                    {
                        var command = msgRequest.Commands[commands.Count - 1];

                        var resultCommand = _commandsStorageEngineExecute[command.Code](command);

                        if (resultCommand != null)
                            resultCommands.Add(resultCommand);
                    }
                }
                catch (Exception e)
                {
                    resultCommands.Add(new ExceptionCommand(e.Message));
                }

                var ms = new MemoryStream();
                var writer = new BinaryWriter(ms);

                var responseClientDescription = new Descriptor(-1, "", StructureType.RESERVED, DataType.Boolean, DataType.Boolean, null, null, DateTime.Now, DateTime.Now, DateTime.Now, null);

                var msgResponse = new Message(msgRequest.Description == null ? responseClientDescription : msgRequest.Description, resultCommands);
                msgResponse.Serialize(writer);

                ms.Position = 0;
                order.Value.Response = ms;
                order.Key.PendingPackets.Add(order.Value);
            }
            catch (Exception exc)
            {
                _tcpServer.LogError(exc);
            }
        }

        #region XTable Commands

        private ICommand Replace(XTablePortable table, ICommand command)
        {
            var cmd = (ReplaceCommand)command;
            table.Replace(cmd.Key, cmd.Record);

            return null;
        }

        private ICommand Delete(XTablePortable table, ICommand command)
        {
            var cmd = (DeleteCommand)command;
            table.Delete(cmd.Key);

            return null;
        }

        private ICommand DeleteRange(XTablePortable table, ICommand command)
        {
            var cmd = (DeleteRangeCommand)command;
            table.Delete(cmd.FromKey, cmd.ToKey);

            return null;
        }

        private ICommand InsertOrIgnore(XTablePortable table, ICommand command)
        {
            var cmd = (InsertOrIgnoreCommand)command;
            table.InsertOrIgnore(cmd.Key, cmd.Record);

            return null;
        }

        private ICommand Clear(XTablePortable table, ICommand command)
        {
            table.Clear();

            return null;
        }

        private ICommand TryGet(XTablePortable table, ICommand command)
        {
            var cmd = (TryGetCommand)command;
            IData record = null;

            table.TryGet(cmd.Key, out record);

            return new TryGetCommand(cmd.Key, record);
        }

        private ICommand Forward(XTablePortable table, ICommand command)
        {
            var cmd = (ForwardCommand)command;

            var list = table.Forward(cmd.FromKey, cmd.FromKey != null, cmd.ToKey, cmd.ToKey != null).Take(cmd.PageCount).ToList();

            return new ForwardCommand(cmd.PageCount, cmd.FromKey, cmd.ToKey, list);
        }

        private ICommand Backward(XTablePortable table, ICommand command)
        {
            var cmd = (BackwardCommand)command;

            var list = table.Backward(cmd.FromKey, cmd.FromKey != null, cmd.ToKey, cmd.ToKey != null).Take(cmd.PageCount).ToList();

            return new BackwardCommand(cmd.PageCount, cmd.FromKey, cmd.ToKey, list);
        }

        private ICommand FindNext(XTablePortable table, ICommand command)
        {
            var cmd = (FindNextCommand)command;
            var keyValue = table.FindNext(cmd.Key);

            return new FindNextCommand(cmd.Key, keyValue);
        }

        private ICommand FindAfter(XTablePortable table, ICommand command)
        {
            var cmd = (FindAfterCommand)command;
            var keyValue = table.FindAfter(cmd.Key);

            return new FindAfterCommand(cmd.Key, keyValue);
        }

        private ICommand FindPrev(XTablePortable table, ICommand command)
        {
            var cmd = (FindPrevCommand)command;
            var keyValue = table.FindPrev(cmd.Key);

            return new FindPrevCommand(cmd.Key, keyValue);
        }

        private ICommand FindBefore(XTablePortable table, ICommand command)
        {
            var cmd = (FindBeforeCommand)command;
            var keyValue = table.FindBefore(cmd.Key);

            return new FindBeforeCommand(cmd.Key, keyValue);
        }

        private ICommand FirstRow(XTablePortable table, ICommand command)
        {
            var cmd = table.FirstRow;

            return new FirstRowCommand(cmd);
        }

        private ICommand LastRow(XTablePortable table, ICommand command)
        {
            var cmd = table.LastRow;

            return new LastRowCommand(cmd);
        }

        private ICommand GetXIndexDescriptor(XTablePortable table, ICommand command)
        {
            var cmd = (XTableDescriptorGetCommand)command;
            cmd.Descriptor = table.Descriptor;

            return new XTableDescriptorGetCommand(cmd.Descriptor);
        }

        private ICommand SetXIndexDescriptor(XTablePortable table, ICommand command)
        {
            var cmd = (XTableDescriptorSetCommand)command;
            var descriptor = (Descriptor)cmd.Descriptor;

            if (descriptor.Tag != null)
                table.Descriptor.Tag = descriptor.Tag;

            return new XTableDescriptorSetCommand(descriptor);
        }

        #endregion

        #region StorageEngine Commands

        private ICommand Count(XTablePortable table, ICommand command)
        {
            var count = table.Count();

            return new CountCommand(count);
        }

        private ICommand StorageEngineCommit(ICommand command)
        {
            _storageEngine.Commit();

            return new StorageEngineCommitCommand();
        }

        private ICommand StorageEngineGetEnumerator(ICommand command)
        {
            var list = new List<IDescriptor>();

            foreach (var locator in _storageEngine)
                list.Add(new Descriptor(locator.Id, locator.Name, locator.StructureType, locator.KeyDataType, locator.RecordDataType, locator.KeyType, locator.RecordType, locator.CreateTime, locator.ModifiedTime, locator.AccessTime, locator.Tag));

            return new StorageEngineGetEnumeratorCommand(list);
        }

        private ICommand StorageEngineExist(ICommand command)
        {
            var cmd = (StorageEngineExistsCommand)command;
            var exist = _storageEngine.Exists(cmd.Name);

            return new StorageEngineExistsCommand(exist, cmd.Name);
        }

        private ICommand StorageEngineFindById(ICommand command)
        {
            var cmd = (StorageEngineFindByIdCommand)command;

            var locator = _storageEngine.Find(cmd.Id);

            return new StorageEngineFindByIdCommand(new Descriptor(locator.Id, locator.Name, locator.StructureType, locator.KeyDataType, locator.RecordDataType, locator.KeyType, locator.RecordType, locator.CreateTime, locator.ModifiedTime, locator.AccessTime, locator.Tag), cmd.Id);
        }

        private ICommand StorageEngineFindByNameCommand(ICommand command)
        {
            var cmd = (StorageEngineFindByNameCommand)command;
            cmd.Descriptor = _storageEngine[cmd.Name];

            return new StorageEngineFindByNameCommand(cmd.Name, cmd.Descriptor);
        }

        private ICommand StorageEngineOpenXIndex(ICommand command)
        {
            var cmd = (StorageEngineOpenXIndexCommand)command;
            _storageEngine.OpenXTablePortable(cmd.Name, cmd.KeyType, cmd.RecordType);

            var locator = _storageEngine[cmd.Name];

            return new StorageEngineOpenXIndexCommand(locator.Id);
        }

        private ICommand StorageEngineOpenXFile(ICommand command)
        {
            var cmd = (StorageEngineOpenXFileCommand)command;
            _storageEngine.OpenXFile(cmd.Name);

            var locator = _storageEngine[cmd.Name];

            return new StorageEngineOpenXFileCommand(locator.Id);
        }

        private ICommand StorageEngineDelete(ICommand command)
        {
            var cmd = (StorageEngineDeleteCommand)command;
            _storageEngine.Delete(cmd.Name);

            return new StorageEngineDeleteCommand(cmd.Name);
        }

        private ICommand StorageEngineRename(ICommand command)
        {
            var cmd = (StorageEngineRenameCommand)command;
            _storageEngine.Rename(cmd.Name, cmd.NewName);

            return new StorageEngineRenameCommand(cmd.Name, cmd.NewName);
        }

        private ICommand StorageEngineCount(ICommand command)
        {
            var count = _storageEngine.Count;

            return new StorageEngineCountCommand(count);
        }

        private ICommand StorageEngineGetCacheSize(ICommand command)
        {
            var cacheSize = _storageEngine.CacheSize;

            return new StorageEngineGetCacheSizeCommand(cacheSize);
        }

        private ICommand StorageEngineSetCacheSize(ICommand command)
        {
            var cmd = (StorageEngineSetCacheSizeCommand)command;
            _storageEngine.CacheSize = cmd.CacheSize;

            return new StorageEngineGetCacheSizeCommand(cmd.CacheSize);
        }

        #endregion

        #region Heap Commands

        private ICommand HeapObtainNewHandle(ICommand command)
        {
            var handle = _storageEngine.Heap.ObtainNewHandle();

            return new HeapObtainNewHandleCommand(handle);
        }

        private ICommand HeapReleaseHandle(ICommand command)
        {
            var cmd = (HeapReleaseHandleCommand)command;
            _storageEngine.Heap.Release(cmd.Handle);

            return new HeapReleaseHandleCommand(-1);
        }

        public ICommand HeapExistsHandle(ICommand command)
        {
            var cmd = (HeapExistsHandleCommand)command;
            var exists = _storageEngine.Heap.Exists(cmd.Handle);

            return new HeapExistsHandleCommand(cmd.Handle, exists);
        }

        public ICommand HeapWrite(ICommand command)
        {
            var cmd = (HeapWriteCommand)command;
            _storageEngine.Heap.Write(cmd.Handle, cmd.Buffer, cmd.Index, cmd.Count);

            return new HeapWriteCommand();
        }

        public ICommand HeapRead(ICommand command)
        {
            var cmd = (HeapReadCommand)command;
            var buffer = _storageEngine.Heap.Read(cmd.Handle);

            return new HeapReadCommand(cmd.Handle, buffer);
        }

        public ICommand HeapCommit(ICommand command)
        {
            _storageEngine.Heap.Commit();

            return command;
        }

        public ICommand HeapClose(ICommand command)
        {
            _storageEngine.Heap.Close();

            return command;
        }

        public ICommand HeapGetTag(ICommand command)
        {
            var tag = _storageEngine.Heap.Tag;

            return new HeapGetTagCommand(tag);
        }

        public ICommand HeapSetTag(ICommand command)
        {
            var cmd = (HeapSetTagCommand)command;

            _storageEngine.Heap.Tag = cmd.Buffer;

            return new HeapSetTagCommand();
        }

        public ICommand HeapDataSize(ICommand command)
        {
            var dataSize = _storageEngine.Heap.DataSize;

            return new HeapDataSizeCommand(dataSize);
        }

        public ICommand HeapSize(ICommand command)
        {
            var size = _storageEngine.Heap.Size;

            return new HeapSizeCommand(size);
        }
        #endregion
    }
}