using System.Collections;
using CatDb.Data;
using CatDb.Database;
using CatDb.Remote.Commands;
using CatDb.WaterfallTree;

namespace CatDb.Remote
{
    public class XTableRemote : ITable<IData, IData>
    {
        private readonly int _pageCapacity = 100000;
        private readonly CommandCollection _commands;

        private Descriptor _indexDescriptor;
        private readonly StorageEngineClient _storageEngine;

        internal XTableRemote(StorageEngineClient storageEngine, Descriptor descriptor)
        {
            _storageEngine = storageEngine;
            _indexDescriptor = descriptor;

            _commands = new CommandCollection(100 * 1024);
        }

        ~XTableRemote()
        {
            Flush();
        }

        private void InternalExecute(ICommand command)
        {
            if (_commands.Capacity == 0)
            {
                var commands = new CommandCollection(1) { command };

                var resultCommands = _storageEngine.Execute(_indexDescriptor, commands);
                SetResult(commands, resultCommands);

                return;
            }

            _commands.Add(command);
            if (_commands.Count == _commands.Capacity || command.IsSynchronous)
                Flush();
        }

        public void Execute(ICommand command)
        {
            InternalExecute(command);
        }

        public void Execute(CommandCollection commands)
        {
            for (var i = 0; i < commands.Count; i++)
                Execute(commands[i]);
        }

        public void Flush()
        {
            if (_commands.Count == 0)
            {
                UpdateDescriptor();
                return;
            }

            UpdateDescriptor();

            var result = _storageEngine.Execute(_indexDescriptor, _commands);
            SetResult(_commands, result);

            _commands.Clear();
        }

        #region IIndex<IKey, IRecord>

        public IData this[IData key]
        {
            get
            {
                if (!TryGet(key, out var record))
                    throw new KeyNotFoundException(key.ToString());

                return record;
            }
            set => Replace(key, value);
        }

        public void Replace(IData key, IData record)
        {
            Execute(new ReplaceCommand(key, record));
        }

        public void InsertOrIgnore(IData key, IData record)
        {
            Execute(new InsertOrIgnoreCommand(key, record));
        }

        public void Delete(IData key)
        {
            Execute(new DeleteCommand(key));
        }

        public void Delete(IData fromKey, IData toKey)
        {
            Execute(new DeleteRangeCommand(fromKey, toKey));
        }

        public void Clear()
        {
            Execute(new ClearCommand());
        }

        public bool Exists(IData key)
        {
            return TryGet(key, out _);
        }

        public bool TryGet(IData key, out IData record)
        {
            var command = new TryGetCommand(key);
            Execute(command);

            record = command.Record;

            return record != null;
        }

        public IData Find(IData key)
        {
            TryGet(key, out var record);

            return record;
        }

        public IData TryGetOrDefault(IData key, IData defaultRecord)
        {
            if (!TryGet(key, out var record))
                return defaultRecord;

            return record;
        }

        public KeyValuePair<IData, IData>? FindNext(IData key)
        {
            var command = new FindNextCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public KeyValuePair<IData, IData>? FindAfter(IData key)
        {
            var command = new FindAfterCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public KeyValuePair<IData, IData>? FindPrev(IData key)
        {
            var command = new FindPrevCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public KeyValuePair<IData, IData>? FindBefore(IData key)
        {
            var command = new FindBeforeCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public IEnumerable<KeyValuePair<IData, IData>> Forward()
        {
            return Forward(default(IData), false, default(IData), false);
        }

        public IEnumerable<KeyValuePair<IData, IData>> Forward(IData from, bool hasFrom, IData to, bool hasTo)
        {
            if (hasFrom && hasTo && _indexDescriptor.KeyComparer.Compare(from, to) > 0)
                throw new ArgumentException("from > to");

            from = hasFrom ? from : default(IData);
            to = hasTo ? to : default(IData);

            List<KeyValuePair<IData, IData>> records = null;
            IData nextKey = null;

            var command = new ForwardCommand(_pageCapacity, from, to, null);
            Execute(command);

            records = command.List;
            nextKey = records != null && records.Count == _pageCapacity ? records[records.Count - 1].Key : null;

            while (records != null)
            {
                Task task = null;
                List<KeyValuePair<IData, IData>> commandRecords = null;

                var returnCount = nextKey != null ? records.Count - 1 : records.Count;

                if (nextKey != null)
                {
                    task = Task.Factory.StartNew(() =>
                    {
                        var forwardCommand = new ForwardCommand(_pageCapacity, nextKey, to, null);
                        Execute(forwardCommand);

                        commandRecords = forwardCommand.List;
                        nextKey = commandRecords != null && commandRecords.Count == _pageCapacity ? commandRecords[commandRecords.Count - 1].Key : null;
                    });
                }

                for (var i = 0; i < returnCount; i++)
                    yield return records[i];

                records = null;

                if (task != null)
                    task.Wait();

                if (commandRecords != null)
                    records = commandRecords;
            }
        }

        public IEnumerable<KeyValuePair<IData, IData>> Backward()
        {
            return Backward(default(IData), false, default(IData), false);
        }

        public IEnumerable<KeyValuePair<IData, IData>> Backward(IData to, bool hasTo, IData from, bool hasFrom)
        {
            if (hasFrom && hasTo && _indexDescriptor.KeyComparer.Compare(from, to) > 0)
                throw new ArgumentException("from > to");

            from = hasFrom ? from : default(IData);
            to = hasTo ? to : default(IData);

            List<KeyValuePair<IData, IData>> records = null;
            IData nextKey = null;

            var command = new BackwardCommand(_pageCapacity, to, from, null);
            Execute(command);

            records = command.List;
            nextKey = records != null && records.Count == _pageCapacity ? records[records.Count - 1].Key : null;

            while (records != null)
            {
                Task task = null;
                List<KeyValuePair<IData, IData>> commandRecords = null;

                var returnCount = nextKey != null ? records.Count - 1 : records.Count;

                if (nextKey != null)
                {
                    task = Task.Factory.StartNew(() =>
                    {
                        var backwardCommand = new BackwardCommand(_pageCapacity, nextKey, from, null);
                        Execute(backwardCommand);

                        commandRecords = backwardCommand.List;
                        nextKey = commandRecords != null && commandRecords.Count == _pageCapacity ? commandRecords[commandRecords.Count - 1].Key : null;
                    });
                }

                for (var i = 0; i < returnCount; i++)
                    yield return records[i];

                records = null;

                if (task != null)
                    task.Wait();

                if (commandRecords != null)
                    records = commandRecords;
            }
        }

        public KeyValuePair<IData, IData> FirstRow
        {
            get
            {
                var command = new FirstRowCommand();
                Execute(command);

                return command.Row.Value;
            }
        }

        public KeyValuePair<IData, IData> LastRow
        {
            get
            {
                var command = new LastRowCommand();
                Execute(command);

                return command.Row.Value;
            }
        }

        public long Count()
        {
            var command = new CountCommand();
            Execute(command);

            return command.Count;
        }

        public IEnumerator<KeyValuePair<IData, IData>> GetEnumerator()
        {
            return Forward().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        private void SetResult(CommandCollection commands, CommandCollection resultCommands)
        {
            var command = commands[commands.Count - 1];
            if (!command.IsSynchronous)
                return;

            var resultOperation = resultCommands[resultCommands.Count - 1];

            try
            {
                switch (command.Code)
                {
                    case CommandCode.TRY_GET:
                        ((TryGetCommand)command).Record = ((TryGetCommand)resultOperation).Record;
                        break;
                    case CommandCode.FORWARD:
                        ((ForwardCommand)command).List = ((ForwardCommand)resultOperation).List;
                        break;
                    case CommandCode.BACKWARD:
                        ((BackwardCommand)command).List = ((BackwardCommand)resultOperation).List;
                        break;
                    case CommandCode.FIND_NEXT:
                        ((FindNextCommand)command).KeyValue = ((FindNextCommand)resultOperation).KeyValue;
                        break;
                    case CommandCode.FIND_AFTER:
                        ((FindAfterCommand)command).KeyValue = ((FindAfterCommand)resultOperation).KeyValue;
                        break;
                    case CommandCode.FIND_PREV:
                        ((FindPrevCommand)command).KeyValue = ((FindPrevCommand)resultOperation).KeyValue;
                        break;
                    case CommandCode.FIND_BEFORE:
                        ((FindBeforeCommand)command).KeyValue = ((FindBeforeCommand)resultOperation).KeyValue;
                        break;
                    case CommandCode.FIRST_ROW:
                        ((FirstRowCommand)command).Row = ((FirstRowCommand)resultOperation).Row;
                        break;
                    case CommandCode.LAST_ROW:
                        ((LastRowCommand)command).Row = ((LastRowCommand)resultOperation).Row;
                        break;
                    case CommandCode.COUNT:
                        ((CountCommand)command).Count = ((CountCommand)resultOperation).Count;
                        break;
                    case CommandCode.STORAGE_ENGINE_COMMIT:
                        break;
                    case CommandCode.EXCEPTION:
                        throw new Exception(((ExceptionCommand)command).Exception);
                }
            }
            catch (Exception e)
            {
                throw new Exception(e.ToString());
            }
        }

        public IDescriptor Descriptor
        {
            get => _indexDescriptor;
            set => _indexDescriptor = (Descriptor)value;
        }

        private void GetDescriptor()
        {
            var command = new XTableDescriptorGetCommand(Descriptor);

            var collection = new CommandCollection(1) { command };

            collection = _storageEngine.Execute(Descriptor, collection);
            var resultCommand = (XTableDescriptorGetCommand)collection[0];

            Descriptor = resultCommand.Descriptor;
        }

        private void SetDescriptor()
        {
            var command = new XTableDescriptorSetCommand(Descriptor);

            var collection = new CommandCollection(1) { command };

            collection = _storageEngine.Execute(Descriptor, collection);
        }

        /// <summary>
        /// Updates the local descriptor with the changes from the remote
        /// and retrieves up to date descriptor from the local server.
        /// </summary>
        private void UpdateDescriptor()
        {
            ICommand command = null;
            var collection = new CommandCollection(1);

            // Set the local descriptor
            command = new XTableDescriptorSetCommand(Descriptor);
            collection.Add(command);

            _storageEngine.Execute(Descriptor, collection);

            // Get the local descriptor
            command = new XTableDescriptorGetCommand(Descriptor);
            collection.Clear();

            collection.Add(command);
            collection = _storageEngine.Execute(Descriptor, collection);

            var resultCommand = (XTableDescriptorGetCommand)collection[0];
            Descriptor = resultCommand.Descriptor;
        }
    }
}