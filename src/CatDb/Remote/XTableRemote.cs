// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Collections;
using CatDb.Data;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Remote.Commands;
using CatDb.WaterfallTree;

namespace CatDb.Remote;
public class XTableRemote : ITable<IData, IData>, IRemoteScanTable, IDisposable
{
    // Adaptive/bounded page sizing for Forward/Backward is configured per-connection via
    // StorageEngineClient.ScanOptions (no hard-coded numbers): an unbounded scan grows the page
    // geometrically; a bounded scan (.Take/cursor) pushes the exact limit to the server.
    private RemoteScanOptions ScanOptions => _storageEngine.ScanOptions;
    private readonly CommandCollection _commands;

    // Guards _commands and all operations that read/write the shared command buffer.
    // Multiple services can share the same XTableRemote instance through StressContext.
    private readonly CatDb.General.Threading.ReentrantLock _lock = new();

    private Descriptor _indexDescriptor;
    private readonly StorageEngineClient _storageEngine;
    private RemoteTableIndexManager? _indexManager;

    public ITableIndexManager Indexes
    {
        get
        {
            if (_indexManager == null)
                _indexManager = new RemoteTableIndexManager(this, _storageEngine);
            return _indexManager;
        }
    }

    internal XTableRemote(StorageEngineClient storageEngine, Descriptor descriptor)
    {
        _storageEngine = storageEngine;
        _indexDescriptor = descriptor;

        // KeyComparer and KeyEqualityComparer are runtime-only fields that are never
        // serialized over the wire.  Initialize them from KeyType so that the bounds
        // checks in Forward/Backward don't throw NullReferenceException.
        if (descriptor.KeyType != null && descriptor.KeyComparer == null)
        {
            var keyData = TypeEngine.Default(descriptor.KeyType);
            descriptor.KeyComparer        = keyData.Comparer;
            descriptor.KeyEqualityComparer = keyData.EqualityComparer;
        }

        _commands = new CommandCollection(100 * 1024);
    }

    ~XTableRemote()
    {
        // Best-effort flush: the connection may already be torn down during GC
        // finalization, so swallow everything — callers must Dispose() for a
        // guaranteed flush.
        try { FlushCoreUnsafe(); } catch { }
    }

    public void Dispose()
    {
        using (_lock.Lock())
        {
            FlushCore();
            GC.SuppressFinalize(this);
        }
    }

    private void InternalExecute(ICommand command)
    {
        using (_lock.Lock())
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
                FlushCore();
        }
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
        using (_lock.Lock())
            FlushCore();
    }

    // Must be called with _lock held.
    private void FlushCore()
    {
        FlushCoreUnsafe();
    }

    // Inner flush — does NOT acquire the lock; caller is responsible.
    // May be called from the finalizer without a lock (single-threaded GC context).
    private void FlushCoreUnsafe()
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

            return record!;
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

    public bool TryGet(IData key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out IData? record)
    {
        var command = new TryGetCommand(key);
        Execute(command);

        record = command.Record;

        return record != null;
    }

    public IData? Find(IData key)
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
        // In network mode, concurrent mutations can make bounds appear momentarily
        // inverted between the time the caller computed them and the server round-trip.
        // Return empty rather than throwing — callers just see a transient empty range.
        if (hasFrom && hasTo && _indexDescriptor.KeyComparer.Compare(from, to) > 0)
            yield break;

        from = hasFrom ? from : default(IData);
        to = hasTo ? to : default(IData);

        var opts = ScanOptions;
        var cap = opts.Clamp(opts.InitialPageCapacity);

        var command = new ForwardCommand(cap, from, to, null);
        Execute(command);

        var records = command.List;
        var nextKey = records != null && records.Count == cap ? records[records.Count - 1].Key : null;

        while (records != null)
        {
            var returnCount = nextKey != null ? records.Count - 1 : records.Count;

            for (var i = 0; i < returnCount; i++)
                yield return records[i];

            records = null;

            if (nextKey != null)
            {
                cap = opts.Clamp(cap * opts.PageGrowthFactor);
                var nextCommand = new ForwardCommand(cap, nextKey, to, null);
                Execute(nextCommand);
                records  = nextCommand.List;
                nextKey  = records != null && records.Count == cap ? records[records.Count - 1].Key : null;
            }
        }
    }

    public IEnumerable<KeyValuePair<IData, IData>> Backward()
    {
        return Backward(default(IData), false, default(IData), false);
    }

    public IEnumerable<KeyValuePair<IData, IData>> Backward(IData to, bool hasTo, IData from, bool hasFrom)
    {
        if (hasFrom && hasTo && _indexDescriptor.KeyComparer.Compare(from, to) > 0)
            yield break;

        from = hasFrom ? from : default(IData);
        to = hasTo ? to : default(IData);

        var opts = ScanOptions;
        var cap = opts.Clamp(opts.InitialPageCapacity);

        var command = new BackwardCommand(cap, to, from, null);
        Execute(command);

        var records = command.List;
        var nextKey = records != null && records.Count == cap ? records[records.Count - 1].Key : null;

        while (records != null)
        {
            var returnCount = nextKey != null ? records.Count - 1 : records.Count;

            for (var i = 0; i < returnCount; i++)
                yield return records[i];

            records = null;

            if (nextKey != null)
            {
                cap = opts.Clamp(cap * opts.PageGrowthFactor);
                var nextCommand = new BackwardCommand(cap, nextKey, from, null);
                Execute(nextCommand);
                records  = nextCommand.List;
                nextKey  = records != null && records.Count == cap ? records[records.Count - 1].Key : null;
            }
        }
    }

    /// <summary>
    /// Forward range bounded to at most <paramref name="maxRows"/> rows. Pushes the limit to the
    /// server so a short query (cursor page / <c>.Take(n)</c>) returns in a single round-trip with
    /// no over-fetch. Larger limits page server-side, capped by <see cref="RemoteScanOptions.MaxPageCapacity"/>.
    /// </summary>
    public IEnumerable<KeyValuePair<IData, IData>> ForwardTake(IData from, bool hasFrom, IData to, bool hasTo, int maxRows)
    {
        if (maxRows <= 0)
            yield break;
        if (hasFrom && hasTo && _indexDescriptor.KeyComparer.Compare(from, to) > 0)
            yield break;

        from = hasFrom ? from : default(IData);
        to   = hasTo   ? to   : default(IData);

        var opts = ScanOptions;
        var remaining = maxRows;
        var curFrom = from;

        while (remaining > 0)
        {
            var cap = opts.Clamp(remaining);
            var command = new ForwardCommand(cap, curFrom, to, null);
            Execute(command);

            var records = command.List;
            if (records == null || records.Count == 0)
                yield break;

            var full = records.Count == cap;
            int emit;
            if (full && remaining > records.Count)
            {
                // Need more than this page holds → drop the last row and re-seek from it (the
                // server treats `from` as inclusive, so this avoids a duplicate without a gap).
                emit = records.Count - 1;
                curFrom = records[records.Count - 1].Key;
            }
            else
            {
                emit = Math.Min(records.Count, remaining);
            }

            for (var i = 0; i < emit; i++)
                yield return records[i];

            remaining -= emit;

            if (!full || emit == 0)
                yield break; // reached end of data (or nothing left to make progress)
        }
    }

    /// <summary>Backward counterpart of <see cref="ForwardTake"/>.</summary>
    public IEnumerable<KeyValuePair<IData, IData>> BackwardTake(IData to, bool hasTo, IData from, bool hasFrom, int maxRows)
    {
        if (maxRows <= 0)
            yield break;
        if (hasFrom && hasTo && _indexDescriptor.KeyComparer.Compare(from, to) > 0)
            yield break;

        from = hasFrom ? from : default(IData);
        to   = hasTo   ? to   : default(IData);

        var opts = ScanOptions;
        var remaining = maxRows;
        var curTo = to;

        while (remaining > 0)
        {
            var cap = opts.Clamp(remaining);
            var command = new BackwardCommand(cap, curTo, from, null);
            Execute(command);

            var records = command.List;
            if (records == null || records.Count == 0)
                yield break;

            var full = records.Count == cap;
            int emit;
            if (full && remaining > records.Count)
            {
                emit = records.Count - 1;
                curTo = records[records.Count - 1].Key;
            }
            else
            {
                emit = Math.Min(records.Count, remaining);
            }

            for (var i = 0; i < emit; i++)
                yield return records[i];

            remaining -= emit;

            if (!full || emit == 0)
                yield break;
        }
    }

    public KeyValuePair<IData, IData>? FirstRow
    {
        get
        {
            var command = new FirstRowCommand();
            Execute(command);

            return command.Row;
        }
    }

    public KeyValuePair<IData, IData>? LastRow
    {
        get
        {
            var command = new LastRowCommand();
            Execute(command);

            return command.Row;
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

        // The server returns an ExceptionCommand (code EXCEPTION) when any command in the
        // batch threw — its code won't match the request command's code, so check the RESULT
        // here and surface the real server message instead of mis-casting it below.
        if (resultOperation.Code == CommandCode.EXCEPTION)
            throw new Exception(((ExceptionCommand)resultOperation).Exception);

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
                case CommandCode.INDEX_FIND:
                    ((IndexFindCommand)command).Results = ((IndexFindCommand)resultOperation).Results;
                    break;
                case CommandCode.INDEX_FIND_RANGE:
                    ((IndexFindRangeCommand)command).Results = ((IndexFindRangeCommand)resultOperation).Results;
                    break;
                case CommandCode.INDEX_FIND_PREFIX:
                    ((IndexFindPrefixCommand)command).Results = ((IndexFindPrefixCommand)resultOperation).Results;
                    break;
                case CommandCode.INDEX_EXISTS:
                    ((IndexExistsCommand)command).Result = ((IndexExistsCommand)resultOperation).Result;
                    break;
                case CommandCode.INDEX_COUNT:
                    ((IndexCountCommand)command).Result = ((IndexCountCommand)resultOperation).Result;
                    break;
                case CommandCode.INDEX_LIST:
                    ((IndexListCommand)command).Results = ((IndexListCommand)resultOperation).Results;
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

        // The server-returned descriptor is serialized over the wire — runtime-only fields
        // (comparers, persist helpers) are not serialized and arrive as null.
        // Restore them from the current descriptor before replacing.
        var returned = (Descriptor)resultCommand.Descriptor;
        returned.KeyComparer          = _indexDescriptor.KeyComparer;
        returned.KeyEqualityComparer  = _indexDescriptor.KeyEqualityComparer;
        returned.KeyPersist           = _indexDescriptor.KeyPersist;
        returned.RecordPersist        = _indexDescriptor.RecordPersist;
        returned.KeyIndexerPersist    = _indexDescriptor.KeyIndexerPersist;
        returned.RecordIndexerPersist = _indexDescriptor.RecordIndexerPersist;

        Descriptor = returned;
    }
}
