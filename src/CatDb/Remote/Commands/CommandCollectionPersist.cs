// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;
using CatDb.General.Persist;

namespace CatDb.Remote.Commands;
public class CommandCollectionPersist : ICommandCollectionPersist
{
    public IPersist<ICommand> Persist { get; private set; }

    public CommandCollectionPersist(IPersist<ICommand> persist)
    {
        Persist = persist;
    }

    public void Write(BinaryWriter writer, CommandCollection collection)
    {
        var collectionCount = collection.Count;
        var commonAction = collection.CommonAction;

        writer.Write(collectionCount);
        writer.Write(commonAction);

        if (collectionCount > 1 && commonAction > 0)
        {
            switch (commonAction)
            {
                case CommandCode.REPLACE:
                case CommandCode.INSERT_OR_IGNORE:
                case CommandCode.DELETE:
                case CommandCode.DELETE_RANGE:
                case CommandCode.CLEAR:
                    {
                        for (var i = 0; i < collectionCount; i++)
                            Persist.Write(writer, collection[i]);
                    }
                    break;

                default:
                    throw new NotImplementedException("Command is not implemented");
            }
        }
        else
        {
            foreach (var command in collection)
                Persist.Write(writer, command);
        }
    }

    public CommandCollection Read(BinaryReader reader)
    {
        var collectionCount = reader.ReadInt32();
        var commonAction = reader.ReadInt32();

        var collection = new CommandCollection(collectionCount);

        if (collectionCount > 1 && commonAction > 0)
        {
            switch (commonAction)
            {
                case CommandCode.REPLACE:
                case CommandCode.INSERT_OR_IGNORE:
                case CommandCode.DELETE:
                case CommandCode.DELETE_RANGE:
                case CommandCode.CLEAR:
                    {
                        for (var i = 0; i < collectionCount; i++)
                            collection.Add(Persist.Read(reader));
                    }
                    break;

                default:
                    throw new NotImplementedException("Command is not implemented");
            }
        }
        else
        {
            for (var i = 0; i < collectionCount; i++)
                collection.Add(Persist.Read(reader));
        }

        return collection;
    }
}

public partial class CommandPersist : IPersist<ICommand>
{
    private readonly Action<BinaryWriter, ICommand>[] _writes;
    private readonly Func<BinaryReader, ICommand>[] _reads;

    public IPersist<IData> KeyPersist { get; private set; }
    public IPersist<IData> RecordPersist { get; private set; }

    public CommandPersist(IPersist<IData> keyPersist, IPersist<IData> recordPersist)
    {
        KeyPersist = keyPersist;
        RecordPersist = recordPersist;

        // XTable writers
        _writes = new Action<BinaryWriter, ICommand>[CommandCode.MAX];
        _writes[CommandCode.REPLACE] = WriteReplaceCommand;
        _writes[CommandCode.DELETE] = WriteDeleteCommand;
        _writes[CommandCode.DELETE_RANGE] = WriteDeleteRangeCommand;
        _writes[CommandCode.INSERT_OR_IGNORE] = WriteInsertOrIgnoreCommand;
        _writes[CommandCode.CLEAR] = WriteClearCommand;
        _writes[CommandCode.TRY_GET] = WriteTryGetCommand;
        _writes[CommandCode.FORWARD] = WriteForwardCommand;
        _writes[CommandCode.BACKWARD] = WriteBackwardCommand;
        _writes[CommandCode.RANGE_COUNT] = WriteRangeCountCommand;
        _writes[CommandCode.FIND_NEXT] = WriteFindNextCommand;
        _writes[CommandCode.FIND_AFTER] = WriteFindAfterCommand;
        _writes[CommandCode.FIND_PREV] = WriteFindPrevCommand;
        _writes[CommandCode.FIND_BEFORE] = WriteFindBeforeCommand;
        _writes[CommandCode.FIRST_ROW] = WriteFirstRowCommand;
        _writes[CommandCode.LAST_ROW] = WriteLastRowCommand;
        _writes[CommandCode.COUNT] = WriteCountCommand;
        _writes[CommandCode.XTABLE_DESCRIPTOR_GET] = WriteXIndexDescriptorGetCommand;
        _writes[CommandCode.XTABLE_DESCRIPTOR_SET] = WriteXIndexDescriptorSetCommand;

        // XTable reads
        _reads = new Func<BinaryReader, ICommand>[CommandCode.MAX];
        _reads[CommandCode.REPLACE] = ReadReplaceCommand;
        _reads[CommandCode.DELETE] = ReadDeleteCommand;
        _reads[CommandCode.DELETE_RANGE] = ReadDeleteRangeCommand;
        _reads[CommandCode.INSERT_OR_IGNORE] = ReadInsertOrIgnoreCommand;
        _reads[CommandCode.CLEAR] = ReadClearCommand;
        _reads[CommandCode.TRY_GET] = ReadTryGetCommand;
        _reads[CommandCode.FORWARD] = ReadForwardCommand;
        _reads[CommandCode.BACKWARD] = ReadBackwardCommand;
        _reads[CommandCode.RANGE_COUNT] = ReadRangeCountCommand;
        _reads[CommandCode.FIND_NEXT] = ReadFindNextCommand;
        _reads[CommandCode.FIND_AFTER] = ReadFindAfterCommand;
        _reads[CommandCode.FIND_PREV] = ReadFindPrevCommand;
        _reads[CommandCode.FIND_BEFORE] = ReadFindBeforeCommand;
        _reads[CommandCode.FIRST_ROW] = ReadFirstRowCommand;
        _reads[CommandCode.LAST_ROW] = ReadLastRowCommand;
        _reads[CommandCode.COUNT] = ReadCountCommand;
        _reads[CommandCode.XTABLE_DESCRIPTOR_GET] = ReadXIndexDescriptorGetCommand;
        _reads[CommandCode.XTABLE_DESCRIPTOR_SET] = ReadXIndexDescriptorSetCommand;

        // Storage engine writes
        _writes[CommandCode.STORAGE_ENGINE_COMMIT] = WriteStorageEngineCommitCommand;
        _writes[CommandCode.STORAGE_ENGINE_GET_ENUMERATOR] = WriteStorageEngineGetEnumeratorCommand;
        _writes[CommandCode.STORAGE_ENGINE_RENAME] = WriteStorageEngineRenameCommand;
        _writes[CommandCode.STORAGE_ENGINE_EXISTS] = WriteStorageEngineExistCommand;
        _writes[CommandCode.STORAGE_ENGINE_FIND_BY_ID] = WriteStorageEngineFindByIdCommand;
        _writes[CommandCode.STORAGE_ENGINE_FIND_BY_NAME] = WriteStorageEngineFindByNameCommand;
        _writes[CommandCode.STORAGE_ENGINE_OPEN_XTABLE] = WriteStorageEngineOpenXIndexCommand;
        _writes[CommandCode.STORAGE_ENGINE_OPEN_XFILE] = WriteStorageEngineOpenXFileCommand;
        _writes[CommandCode.STORAGE_ENGINE_DELETE] = WriteStorageEngineDeleteCommand;
        _writes[CommandCode.STORAGE_ENGINE_COUNT] = WriteStorageEngineCountCommand;
        _writes[CommandCode.STORAGE_ENGINE_DESCRIPTOR] = WriteStorageEngineDescriptionCommand;
        _writes[CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE] = WriteStorageEngineGetCacheCommand;
        _writes[CommandCode.STORAGE_ENGINE_SET_CACHE_SIZE] = WriteStorageEngineSetCacheCommand;

        // Storage engine reads
        _reads[CommandCode.STORAGE_ENGINE_COMMIT] = ReadStorageEngineCommitCommand;
        _reads[CommandCode.STORAGE_ENGINE_GET_ENUMERATOR] = ReadStorageEngineGetEnumeratorCommand;
        _reads[CommandCode.STORAGE_ENGINE_RENAME] = ReadStorageEngineRenameCommand;
        _reads[CommandCode.STORAGE_ENGINE_EXISTS] = ReadStorageEngineExistCommand;
        _reads[CommandCode.STORAGE_ENGINE_FIND_BY_ID] = ReadStorageEngineFindByIdCommand;
        _reads[CommandCode.STORAGE_ENGINE_FIND_BY_NAME] = ReadStorageEngineFindByNameCommand;
        _reads[CommandCode.STORAGE_ENGINE_OPEN_XTABLE] = ReadStorageEngineOpenXIndexCommand;
        _reads[CommandCode.STORAGE_ENGINE_OPEN_XFILE] = ReadStorageEngineOpenXFileCommand;
        _reads[CommandCode.STORAGE_ENGINE_DELETE] = ReadStorageEngineDeleteCommand;
        _reads[CommandCode.STORAGE_ENGINE_COUNT] = ReadStorageEngineCountCommand;
        _reads[CommandCode.STORAGE_ENGINE_DESCRIPTOR] = ReadStorageEngineDescriptionCommand;
        _reads[CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE] = ReadStorageEngineGetCacheSizeCommand;
        _reads[CommandCode.STORAGE_ENGINE_SET_CACHE_SIZE] = ReadStorageEngineSetCacheCommand;

        //Heap writes
        _writes[CommandCode.HEAP_OBTAIN_NEW_HANDLE] = WriteHeapObtainNewHandleCommand;
        _writes[CommandCode.HEAP_RELEASE_HANDLE] = WriteHeapReleaseHandleCommand;
        _writes[CommandCode.HEAP_EXISTS_HANDLE] = WriteHeapExistsHandleCommand;
        _writes[CommandCode.HEAP_WRITE] = WriteHeapWriteCommand;
        _writes[CommandCode.HEAP_READ] = WriteHeapReadCommand;
        _writes[CommandCode.HEAP_COMMIT] = WriteHeapCommitCommand;
        _writes[CommandCode.HEAP_CLOSE] = WriteHeapCloseCommand;
        _writes[CommandCode.HEAP_SET_TAG] = WriteHeapSetTagCommand;
        _writes[CommandCode.HEAP_GET_TAG] = WriteHeapGetTagCommand;
        _writes[CommandCode.HEAP_DATA_SIZE] = WriteHeapDataSizeCommand;
        _writes[CommandCode.HEAP_SIZE] = WriteHeapSizeCommand;

        //Heap reads
        _reads[CommandCode.HEAP_OBTAIN_NEW_HANDLE] = ReadHeapObtainNewHandleCommand;
        _reads[CommandCode.HEAP_RELEASE_HANDLE] = ReadHeapReleaseHandleCommand;
        _reads[CommandCode.HEAP_EXISTS_HANDLE] = ReadHeapExistsHandleCommand;
        _reads[CommandCode.HEAP_WRITE] = ReadHeapWriteCommand;
        _reads[CommandCode.HEAP_READ] = ReadHeapReadCommand;
        _reads[CommandCode.HEAP_COMMIT] = ReadHeapCommitCommand;
        _reads[CommandCode.HEAP_CLOSE] = ReadHeapCloseCommand;
        _reads[CommandCode.HEAP_SET_TAG] = ReadHeapSetTagCommand;
        _reads[CommandCode.HEAP_GET_TAG] = ReadHeapGetTagCommand;
        _reads[CommandCode.HEAP_DATA_SIZE] = ReadHeapDataSizeCommand;
        _reads[CommandCode.HEAP_SIZE] = ReadHeapSizeCommand;

        // Index command writes
        _writes[CommandCode.INDEX_CREATE] = WriteIndexCreateCommand;
        _writes[CommandCode.INDEX_DROP] = WriteIndexDropCommand;
        _writes[CommandCode.INDEX_FIND] = WriteIndexFindCommand;
        _writes[CommandCode.INDEX_FIND_RANGE] = WriteIndexFindRangeCommand;
        _writes[CommandCode.INDEX_FIND_PREFIX] = WriteIndexFindPrefixCommand;
        _writes[CommandCode.INDEX_EXISTS] = WriteIndexExistsCommand;
        _writes[CommandCode.INDEX_COUNT] = WriteIndexCountCommand;
        _writes[CommandCode.INDEX_REBUILD] = WriteIndexRebuildCommand;
        _writes[CommandCode.INDEX_LIST] = WriteIndexListCommand;
        _writes[CommandCode.INDEX_QUERY] = WriteIndexQueryCommand;
        _writes[CommandCode.INDEX_COUNT_QUERY] = WriteIndexCountQueryCommand;

        // Index command reads
        _reads[CommandCode.INDEX_CREATE] = ReadIndexCreateCommand;
        _reads[CommandCode.INDEX_DROP] = ReadIndexDropCommand;
        _reads[CommandCode.INDEX_FIND] = ReadIndexFindCommand;
        _reads[CommandCode.INDEX_FIND_RANGE] = ReadIndexFindRangeCommand;
        _reads[CommandCode.INDEX_FIND_PREFIX] = ReadIndexFindPrefixCommand;
        _reads[CommandCode.INDEX_EXISTS] = ReadIndexExistsCommand;
        _reads[CommandCode.INDEX_COUNT] = ReadIndexCountCommand;
        _reads[CommandCode.INDEX_REBUILD] = ReadIndexRebuildCommand;
        _reads[CommandCode.INDEX_LIST] = ReadIndexListCommand;
        _reads[CommandCode.INDEX_QUERY] = ReadIndexQueryCommand;
        _reads[CommandCode.INDEX_COUNT_QUERY] = ReadIndexCountQueryCommand;

        _writes[CommandCode.EXCEPTION] = WriteExceptionCommand;
        _reads[CommandCode.EXCEPTION] = ReadExceptionCommand;
    }

    public void Write(BinaryWriter writer, ICommand item)
    {
        writer.Write(item.Code);
        _writes[item.Code](writer, item);
    }

    public ICommand Read(BinaryReader reader)
    {
        var code = reader.ReadInt32();

        return _reads[code](reader);
    }
}
