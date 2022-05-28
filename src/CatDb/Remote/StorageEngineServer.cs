using CatDb.Data;
using CatDb.Database;
using CatDb.WaterfallTree;
using CatDb.General.Communication;
using CatDb.Remote.Commands;

namespace CatDb.Remote
{
    public class StorageEngineServer
    {
        private CancellationTokenSource ShutdownTokenSource;
        private Thread Worker;

        private Func<XTablePortable, ICommand, ICommand>[] CommandsIIndexExecute;
        private Func<ICommand, ICommand>[] CommandsStorageEngineExecute;
        private Func<ICommand, ICommand>[] CommandsHeapExecute;

        public readonly IStorageEngine StorageEngine;
        public readonly TcpServer TcpServer;

        public StorageEngineServer(IStorageEngine storageEngine, TcpServer tcpServer)
        {
            if (storageEngine == null)
                throw new ArgumentNullException("storageEngine");
            if (tcpServer == null)
                throw new ArgumentNullException("tcpServer");

            StorageEngine = storageEngine;
            TcpServer = tcpServer;

            CommandsIIndexExecute = new Func<XTablePortable, ICommand, ICommand>[CommandCode.MAX];
            CommandsIIndexExecute[CommandCode.REPLACE] = Replace;
            CommandsIIndexExecute[CommandCode.DELETE] = Delete;
            CommandsIIndexExecute[CommandCode.DELETE_RANGE] = DeleteRange;
            CommandsIIndexExecute[CommandCode.INSERT_OR_IGNORE] = InsertOrIgnore;
            CommandsIIndexExecute[CommandCode.CLEAR] = Clear;
            CommandsIIndexExecute[CommandCode.TRY_GET] = TryGet;
            CommandsIIndexExecute[CommandCode.FORWARD] = Forward;
            CommandsIIndexExecute[CommandCode.BACKWARD] = Backward;
            CommandsIIndexExecute[CommandCode.FIND_NEXT] = FindNext;
            CommandsIIndexExecute[CommandCode.FIND_AFTER] = FindAfter;
            CommandsIIndexExecute[CommandCode.FIND_PREV] = FindPrev;
            CommandsIIndexExecute[CommandCode.FIND_BEFORE] = FindBefore;
            CommandsIIndexExecute[CommandCode.FIRST_ROW] = FirstRow;
            CommandsIIndexExecute[CommandCode.LAST_ROW] = LastRow;
            CommandsIIndexExecute[CommandCode.COUNT] = Count;
            CommandsIIndexExecute[CommandCode.XTABLE_DESCRIPTOR_GET] = GetXIndexDescriptor;
            CommandsIIndexExecute[CommandCode.XTABLE_DESCRIPTOR_SET] = SetXIndexDescriptor;

            CommandsStorageEngineExecute = new Func<ICommand, ICommand>[CommandCode.MAX];
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_COMMIT] = StorageEngineCommit;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_GET_ENUMERATOR] = StorageEngineGetEnumerator;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_RENAME] = StorageEngineRename;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_EXISTS] = StorageEngineExist;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_FIND_BY_ID] = StorageEngineFindByID;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_FIND_BY_NAME] = StorageEngineFindByNameCommand;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_OPEN_XTABLE] = StorageEngineOpenXIndex;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_OPEN_XFILE] = StorageEngineOpenXFile;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_DELETE] = StorageEngineDelete;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_COUNT] = StorageEngineCount;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE] = StorageEngineGetCacheSize;
            CommandsStorageEngineExecute[CommandCode.STORAGE_ENGINE_SET_CACHE_SIZE] = StorageEngineSetCacheSize;
            CommandsStorageEngineExecute[CommandCode.HEAP_OBTAIN_NEW_HANDLE] = HeapObtainNewHandle;
            CommandsStorageEngineExecute[CommandCode.HEAP_RELEASE_HANDLE] = HeapReleaseHandle;
            CommandsStorageEngineExecute[CommandCode.HEAP_EXISTS_HANDLE] = HeapExistsHandle;
            CommandsStorageEngineExecute[CommandCode.HEAP_WRITE] = HeapWrite;
            CommandsStorageEngineExecute[CommandCode.HEAP_READ] = HeapRead;
            CommandsStorageEngineExecute[CommandCode.HEAP_COMMIT] = HeapCommit;
            CommandsStorageEngineExecute[CommandCode.HEAP_CLOSE] = HeapClose;
            CommandsStorageEngineExecute[CommandCode.HEAP_GET_TAG] = HeapGetTag;
            CommandsStorageEngineExecute[CommandCode.HEAP_SET_TAG] = HeapSetTag;
            CommandsStorageEngineExecute[CommandCode.HEAP_DATA_SIZE] = HeapDataSize;
            CommandsStorageEngineExecute[CommandCode.HEAP_SIZE] = HeapSize;
        }

        public void Start()
        {
            Stop();

            ShutdownTokenSource = new CancellationTokenSource();

            Worker = new Thread(DoWork);
            Worker.Start();
        }

        public void Stop()
        {
            if (!IsWorking)
                return;

            ShutdownTokenSource.Cancel(false);

            var thread = Worker;
            if (thread != null)
            {
                if (!thread.Join(5000))
                    thread.Abort();
            }
        }

        public bool IsWorking => Worker != null;

        private void DoWork()
        {
            try
            {
                TcpServer.Start();

                while (!ShutdownTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        var order = TcpServer.RecievedPackets.Take(ShutdownTokenSource.Token);
                        Task.Factory.StartNew(PacketExecute, order);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception exc)
                    {
                        TcpServer.LogError(exc);
                    }
                }
            }
            catch (Exception exc)
            {
                TcpServer.LogError(exc);
            }
            finally
            {
                TcpServer.Stop();

                Worker = null;
            }
        }

        private void PacketExecute(object state)
        {
            try
            {
                var order = (KeyValuePair<ServerConnection, Packet>)state;

                var reader = new BinaryReader(order.Value.Request);
                var msgRequest = Message.Deserialize(reader, (id) => StorageEngine.Find(id));

                var clientDescription = msgRequest.Description;
                var resultCommands = new CommandCollection(1);

                try
                {
                    var commands = msgRequest.Commands;

                    if (msgRequest.Description != null) // XTable commands
                    {
                        var table = (XTablePortable)StorageEngine.OpenXTablePortable(clientDescription.Name, clientDescription.KeyDataType, clientDescription.RecordDataType);
                        table.Descriptor.Tag = clientDescription.Tag;

                        for (var i = 0; i < commands.Count - 1; i++)
                        {
                            var command = msgRequest.Commands[i];
                            CommandsIIndexExecute[command.Code](table, command);
                        }

                        var resultCommand = CommandsIIndexExecute[msgRequest.Commands[commands.Count - 1].Code](table, msgRequest.Commands[commands.Count - 1]);
                        if (resultCommand != null)
                            resultCommands.Add(resultCommand);

                        table.Flush();
                    }
                    else //Storage engine commands
                    {
                        var command = msgRequest.Commands[commands.Count - 1];

                        var resultCommand = CommandsStorageEngineExecute[command.Code](command);

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
                TcpServer.LogError(exc);
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

            var exist = table.TryGet(cmd.Key, out record);

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
            StorageEngine.Commit();

            return new StorageEngineCommitCommand();
        }

        private ICommand StorageEngineGetEnumerator(ICommand command)
        {
            var list = new List<IDescriptor>();

            foreach (var locator in StorageEngine)
                list.Add(new Descriptor(locator.ID, locator.Name, locator.StructureType, locator.KeyDataType, locator.RecordDataType, locator.KeyType, locator.RecordType, locator.CreateTime, locator.ModifiedTime, locator.AccessTime, locator.Tag));

            return new StorageEngineGetEnumeratorCommand(list);
        }

        private ICommand StorageEngineExist(ICommand command)
        {
            var cmd = (StorageEngineExistsCommand)command;
            var exist = StorageEngine.Exists(cmd.Name);

            return new StorageEngineExistsCommand(exist, cmd.Name);
        }

        private ICommand StorageEngineFindByID(ICommand command)
        {
            var cmd = (StorageEngineFindByIDCommand)command;

            var locator = StorageEngine.Find(cmd.ID);

            return new StorageEngineFindByIDCommand(new Descriptor(locator.ID, locator.Name, locator.StructureType, locator.KeyDataType, locator.RecordDataType, locator.KeyType, locator.RecordType, locator.CreateTime, locator.ModifiedTime, locator.AccessTime, locator.Tag), cmd.ID);
        }

        private ICommand StorageEngineFindByNameCommand(ICommand command)
        {
            var cmd = (StorageEngineFindByNameCommand)command;
            cmd.Descriptor = StorageEngine[cmd.Name];

            return new StorageEngineFindByNameCommand(cmd.Name, cmd.Descriptor);
        }

        private ICommand StorageEngineOpenXIndex(ICommand command)
        {
            var cmd = (StorageEngineOpenXIndexCommand)command;
            StorageEngine.OpenXTablePortable(cmd.Name, cmd.KeyType, cmd.RecordType);

            var locator = StorageEngine[cmd.Name];

            return new StorageEngineOpenXIndexCommand(locator.ID);
        }

        private ICommand StorageEngineOpenXFile(ICommand command)
        {
            var cmd = (StorageEngineOpenXFileCommand)command;
            StorageEngine.OpenXFile(cmd.Name);

            var locator = StorageEngine[cmd.Name];

            return new StorageEngineOpenXFileCommand(locator.ID);
        }

        private ICommand StorageEngineDelete(ICommand command)
        {
            var cmd = (StorageEngineDeleteCommand)command;
            StorageEngine.Delete(cmd.Name);

            return new StorageEngineDeleteCommand(cmd.Name);
        }

        private ICommand StorageEngineRename(ICommand command)
        {
            var cmd = (StorageEngineRenameCommand)command;
            StorageEngine.Rename(cmd.Name, cmd.NewName);

            return new StorageEngineRenameCommand(cmd.Name, cmd.NewName);
        }

        private ICommand StorageEngineCount(ICommand command)
        {
            var cmd = (StorageEngineCountCommand)command;
            var count = StorageEngine.Count;

            return new StorageEngineCountCommand(count);
        }

        private ICommand StorageEngineGetCacheSize(ICommand command)
        {
            var cacheSize = StorageEngine.CacheSize;

            return new StorageEngineGetCacheSizeCommand(cacheSize);
        }

        private ICommand StorageEngineSetCacheSize(ICommand command)
        {
            var cmd = (StorageEngineSetCacheSizeCommand)command;
            StorageEngine.CacheSize = cmd.CacheSize;

            return new StorageEngineGetCacheSizeCommand(cmd.CacheSize);
        }

        #endregion

        #region Heap Commands

        private ICommand HeapObtainNewHandle(ICommand command)
        {
            var handle = StorageEngine.Heap.ObtainNewHandle();

            return new HeapObtainNewHandleCommand(handle);
        }

        private ICommand HeapReleaseHandle(ICommand command)
        {
            var cmd = (HeapReleaseHandleCommand)command;
            StorageEngine.Heap.Release(cmd.Handle);

            return new HeapReleaseHandleCommand(-1);
        }

        public ICommand HeapExistsHandle(ICommand command)
        {
            var cmd = (HeapExistsHandleCommand)command;
            var exists = StorageEngine.Heap.Exists(cmd.Handle);

            return new HeapExistsHandleCommand(cmd.Handle, exists);
        }

        public ICommand HeapWrite(ICommand command)
        {
            var cmd = (HeapWriteCommand)command;
            StorageEngine.Heap.Write(cmd.Handle, cmd.Buffer, cmd.Index, cmd.Count);

            return new HeapWriteCommand();
        }

        public ICommand HeapRead(ICommand command)
        {
            var cmd = (HeapReadCommand)command;
            var buffer = StorageEngine.Heap.Read(cmd.Handle);

            return new HeapReadCommand(cmd.Handle, buffer);
        }

        public ICommand HeapCommit(ICommand command)
        {
            StorageEngine.Heap.Commit();

            return command;
        }

        public ICommand HeapClose(ICommand command)
        {
            StorageEngine.Heap.Close();

            return command;
        }

        public ICommand HeapGetTag(ICommand command)
        {
            var tag = StorageEngine.Heap.Tag;

            return new HeapGetTagCommand(tag);
        }

        public ICommand HeapSetTag(ICommand command)
        {
            var cmd = (HeapSetTagCommand)command;

            StorageEngine.Heap.Tag = cmd.Buffer;

            return new HeapSetTagCommand();
        }

        public ICommand HeapDataSize(ICommand command)
        {
            var dataSize = StorageEngine.Heap.DataSize;

            return new HeapDataSizeCommand(dataSize);
        }

        public ICommand HeapSize(ICommand command)
        {
            var size = StorageEngine.Heap.Size;

            return new HeapSizeCommand(size);
        }
        #endregion
    }
}