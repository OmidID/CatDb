// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;
using CatDb.General.Compression;
using CatDb.WaterfallTree;

namespace CatDb.Remote.Commands;
public partial class CommandPersist
{
    #region XIndex Commands

    private void WriteReplaceCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (ReplaceCommand)command;

        KeyPersist.Write(writer, cmd.Key);
        RecordPersist.Write(writer, cmd.Record);
    }

    private ReplaceCommand ReadReplaceCommand(BinaryReader reader)
    {
        return new ReplaceCommand(KeyPersist.Read(reader), RecordPersist.Read(reader));
    }

    private void WriteDeleteCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (DeleteCommand)command;

        KeyPersist.Write(writer, cmd.Key);
    }

    private DeleteCommand ReadDeleteCommand(BinaryReader reader)
    {
        return new DeleteCommand(KeyPersist.Read(reader));
    }

    private void WriteDeleteRangeCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (DeleteRangeCommand)command;

        KeyPersist.Write(writer, cmd.FromKey);
        KeyPersist.Write(writer, cmd.ToKey);
    }

    private DeleteRangeCommand ReadDeleteRangeCommand(BinaryReader reader)
    {
        return new DeleteRangeCommand(KeyPersist.Read(reader), KeyPersist.Read(reader));
    }

    private void WriteClearCommand(BinaryWriter writer, ICommand command)
    {
    }

    private ClearCommand ReadClearCommand(BinaryReader reader)
    {
        return new ClearCommand();
    }

    private void WriteInsertOrIgnoreCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (InsertOrIgnoreCommand)command;

        KeyPersist.Write(writer, cmd.Key);
        RecordPersist.Write(writer, cmd.Record);
    }

    private InsertOrIgnoreCommand ReadInsertOrIgnoreCommand(BinaryReader reader)
    {
        return new InsertOrIgnoreCommand(KeyPersist.Read(reader), RecordPersist.Read(reader));
    }

    private void WriteTryGetCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (TryGetCommand)command;

        KeyPersist.Write(writer, cmd.Key);

        writer.Write(cmd.Record != null);
        if (cmd.Record != null)
            RecordPersist.Write(writer, cmd.Record);
    }

    private TryGetCommand ReadTryGetCommand(BinaryReader reader)
    {
        return new TryGetCommand(KeyPersist.Read(reader), reader.ReadBoolean() ? RecordPersist.Read(reader) : null);
    }

    private void WriteForwardCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (ForwardCommand)command;

        writer.Write(cmd.PageCount);

        writer.Write(cmd.FromKey != null);
        if (cmd.FromKey != null)
            KeyPersist.Write(writer, cmd.FromKey);

        writer.Write(cmd.ToKey != null);
        if (cmd.ToKey != null)
            KeyPersist.Write(writer, cmd.ToKey);

        writer.Write(cmd.List != null);
        if (cmd.List != null)
            SerializeList(writer, cmd.List, cmd.List.Count);
    }

    private ForwardCommand ReadForwardCommand(BinaryReader reader)
    {
        var pageCount = reader.ReadInt32();
        var from = reader.ReadBoolean() ? KeyPersist.Read(reader) : null;
        var to = reader.ReadBoolean() ? KeyPersist.Read(reader) : null;
        var list = reader.ReadBoolean() ? DeserializeList(reader) : null;

        return new ForwardCommand(pageCount, from, to, list);
    }

    private void WriteBackwardCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (BackwardCommand)command;

        writer.Write(cmd.PageCount);

        writer.Write(cmd.FromKey != null);
        if (cmd.FromKey != null)
            KeyPersist.Write(writer, cmd.FromKey);

        writer.Write(cmd.ToKey != null);
        if (cmd.ToKey != null)
            KeyPersist.Write(writer, cmd.ToKey);

        writer.Write(cmd.List != null);
        if (cmd.List != null)
            SerializeList(writer, cmd.List, cmd.List.Count);
    }

    private BackwardCommand ReadBackwardCommand(BinaryReader reader)
    {
        var pageCount = reader.ReadInt32();
        var from = reader.ReadBoolean() ? KeyPersist.Read(reader) : null;
        var to = reader.ReadBoolean() ? KeyPersist.Read(reader) : null;
        var list = reader.ReadBoolean() ? DeserializeList(reader) : null;

        return new BackwardCommand(pageCount, from, to, list);
    }

    private void WriteFindNextCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (FindNextCommand)command;

        KeyPersist.Write(writer, cmd.Key);

        writer.Write(cmd.KeyValue.HasValue);
        if (cmd.KeyValue.HasValue)
        {
            KeyPersist.Write(writer, cmd.KeyValue.Value.Key);
            RecordPersist.Write(writer, cmd.KeyValue.Value.Value);
        }
    }

    private FindNextCommand ReadFindNextCommand(BinaryReader reader)
    {
        var firstKey = KeyPersist.Read(reader);

        var hasValue = reader.ReadBoolean();
        var key = hasValue ? KeyPersist.Read(reader) : null;
        var rec = hasValue ? RecordPersist.Read(reader) : null;

        return new FindNextCommand(firstKey, hasValue ? new KeyValuePair<IData, IData>(key!, rec!) : null);
    }

    private void WriteFindAfterCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (FindAfterCommand)command;

        KeyPersist.Write(writer, cmd.Key);

        writer.Write(cmd.KeyValue.HasValue);
        if (cmd.KeyValue.HasValue)
        {
            KeyPersist.Write(writer, cmd.KeyValue.Value.Key);
            RecordPersist.Write(writer, cmd.KeyValue.Value.Value);
        }
    }

    private FindAfterCommand ReadFindAfterCommand(BinaryReader reader)
    {
        var firstKey = KeyPersist.Read(reader);

        var hasValue = (reader.ReadBoolean());
        var key = hasValue ? KeyPersist.Read(reader) : null;
        var rec = hasValue ? RecordPersist.Read(reader) : null;

        return new FindAfterCommand(firstKey, hasValue ? new KeyValuePair<IData, IData>(key!, rec!) : null);
    }

    private void WriteFindPrevCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (FindPrevCommand)command;

        KeyPersist.Write(writer, cmd.Key);

        writer.Write(cmd.KeyValue.HasValue);
        if (cmd.KeyValue.HasValue)
        {
            KeyPersist.Write(writer, cmd.KeyValue.Value.Key);
            RecordPersist.Write(writer, cmd.KeyValue.Value.Value);
        }
    }

    private FindPrevCommand ReadFindPrevCommand(BinaryReader reader)
    {
        var firstKey = KeyPersist.Read(reader);

        var hasValue = (reader.ReadBoolean());
        var key = hasValue ? KeyPersist.Read(reader) : null;
        var rec = hasValue ? RecordPersist.Read(reader) : null;

        return new FindPrevCommand(firstKey, hasValue ? new KeyValuePair<IData, IData>(key!, rec!) : null);
    }

    private void WriteFindBeforeCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (FindBeforeCommand)command;

        KeyPersist.Write(writer, cmd.Key);

        writer.Write(cmd.KeyValue.HasValue);
        if (cmd.KeyValue.HasValue)
        {
            KeyPersist.Write(writer, cmd.KeyValue.Value.Key);
            RecordPersist.Write(writer, cmd.KeyValue.Value.Value);
        }
    }

    private FindBeforeCommand ReadFindBeforeCommand(BinaryReader reader)
    {
        var firstKey = KeyPersist.Read(reader);

        var hasValue = (reader.ReadBoolean());
        var key = hasValue ? KeyPersist.Read(reader) : null;
        var rec = hasValue ? RecordPersist.Read(reader) : null;

        return new FindBeforeCommand(firstKey, hasValue ? new KeyValuePair<IData, IData>(key!, rec!) : null);
    }

    private void WriteFirstRowCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (FirstRowCommand)command;

        writer.Write(cmd.Row.HasValue);
        if (cmd.Row.HasValue)
        {
            KeyPersist.Write(writer, cmd.Row.Value.Key);
            RecordPersist.Write(writer, cmd.Row.Value.Value);
        }
    }

    private FirstRowCommand ReadFirstRowCommand(BinaryReader reader)
    {
        var hasValue = (reader.ReadBoolean());
        var key = hasValue ? KeyPersist.Read(reader) : null;
        var rec = hasValue ? RecordPersist.Read(reader) : null;

        return new FirstRowCommand(hasValue ? new KeyValuePair<IData, IData>(key!, rec!) : null);
    }

    private void WriteLastRowCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (LastRowCommand)command;

        writer.Write(cmd.Row.HasValue);
        if (cmd.Row.HasValue)
        {
            KeyPersist.Write(writer, cmd.Row.Value.Key);
            RecordPersist.Write(writer, cmd.Row.Value.Value);
        }
    }

    private LastRowCommand ReadLastRowCommand(BinaryReader reader)
    {
        var hasValue = (reader.ReadBoolean());
        var key = hasValue ? KeyPersist.Read(reader) : null;
        var rec = hasValue ? RecordPersist.Read(reader) : null;

        return new LastRowCommand(hasValue ? new KeyValuePair<IData, IData>(key!, rec!) : null);
    }

    private void WriteCountCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (CountCommand)command;
        writer.Write(cmd.Count);
    }

    private CountCommand ReadCountCommand(BinaryReader reader)
    {
        return new CountCommand(reader.ReadInt64());
    }

    private void WriteXIndexDescriptorGetCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (XTableDescriptorGetCommand)command;
        var descriptor = cmd.Descriptor;

        writer.Write(descriptor != null);

        if (descriptor != null)
            SerializeDescriptor(writer, descriptor);
    }

    private XTableDescriptorGetCommand ReadXIndexDescriptorGetCommand(BinaryReader reader)
    {
        IDescriptor? description = null;

        if (reader.ReadBoolean()) // Description != null
            description = Descriptor.Deserialize(reader);

        return new XTableDescriptorGetCommand(description);
    }

    private void WriteXIndexDescriptorSetCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (XTableDescriptorSetCommand)command;
        var descriptor = (Descriptor?)cmd.Descriptor;

        writer.Write(descriptor != null);

        if (descriptor != null)
            descriptor.Serialize(writer);
    }

    private XTableDescriptorSetCommand ReadXIndexDescriptorSetCommand(BinaryReader reader)
    {
        IDescriptor? descriptor = null;

        if (reader.ReadBoolean()) // Descriptor != null
            descriptor = Descriptor.Deserialize(reader);

        return new XTableDescriptorSetCommand(descriptor);
    }

    #endregion

    #region Storage EngineCommands

    private void WriteStorageEngineCommitCommand(BinaryWriter writer, ICommand command)
    {
    }

    private StorageEngineCommitCommand ReadStorageEngineCommitCommand(BinaryReader reader)
    {
        return new StorageEngineCommitCommand();
    }

    private void WriteStorageEngineGetEnumeratorCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineGetEnumeratorCommand)command;

        if (cmd.Descriptions == null)
            writer.Write(true);
        else
        {
            writer.Write(false);

            var listCount = cmd.Descriptions.Count;
            CountCompression.Serialize(writer, (ulong)listCount);

            for (var i = 0; i < listCount; i++)
                SerializeDescriptor(writer, cmd.Descriptions[i]);
        }
    }

    private StorageEngineGetEnumeratorCommand ReadStorageEngineGetEnumeratorCommand(BinaryReader reader)
    {
        var isListNull = reader.ReadBoolean();
        var descriptions = new List<IDescriptor>();

        if (!isListNull)
        {
            var listCount = (int)CountCompression.Deserialize(reader);

            for (var i = 0; i < listCount; i++)
                descriptions.Add((Descriptor)DeserializeDescriptor(reader));
        }

        return new StorageEngineGetEnumeratorCommand(descriptions);
    }

    private void WriteStorageEngineRenameCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineRenameCommand)command;

        writer.Write(cmd.Name);
        writer.Write(cmd.NewName);
    }

    private StorageEngineRenameCommand ReadStorageEngineRenameCommand(BinaryReader reader)
    {
        var name = reader.ReadString();
        var newName = reader.ReadString();

        return new StorageEngineRenameCommand(name, newName);
    }

    private void WriteStorageEngineExistCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineExistsCommand)command;

        writer.Write(cmd.Name);
        writer.Write(cmd.Exist);
    }

    private StorageEngineExistsCommand ReadStorageEngineExistCommand(BinaryReader reader)
    {
        var name = reader.ReadString();
        var exist = reader.ReadBoolean();

        return new StorageEngineExistsCommand(exist, name);
    }

    private void WriteStorageEngineFindByIdCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineFindByIdCommand)command;

        writer.Write(cmd.Id);

        writer.Write(cmd.Descriptor != null);
        if (cmd.Descriptor != null)
            SerializeDescriptor(writer, cmd.Descriptor);
    }

    private StorageEngineFindByIdCommand ReadStorageEngineFindByIdCommand(BinaryReader reader)
    {
        var id = reader.ReadInt64();
        var schemeRecord = reader.ReadBoolean() ? DeserializeDescriptor(reader) : null;

        return new StorageEngineFindByIdCommand(schemeRecord, id);
    }

    private void WriteStorageEngineOpenXIndexCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineOpenXIndexCommand)command;

        writer.Write(cmd.Id);
        if (cmd.Id < 0)
        {
            cmd.KeyType.Serialize(writer);
            cmd.RecordType.Serialize(writer);

            writer.Write(cmd.Name);

            // Member maps (name → slot index) — allows server to persist field names.
            WriteMemberMap(writer, cmd.KeyMembers);
            WriteMemberMap(writer, cmd.RecordMembers);
        }
    }

    private StorageEngineOpenXIndexCommand ReadStorageEngineOpenXIndexCommand(BinaryReader reader)
    {
        var id = reader.ReadInt64();

        if (id < 0)
        {
            var keyType = DataType.Deserialize(reader);
            var recordType = DataType.Deserialize(reader);

            var name = reader.ReadString();

            // Read optional member maps (backward-compatible: new clients always write them).
            var keyMembers = ReadMemberMap(reader);
            var recordMembers = ReadMemberMap(reader);

            return new StorageEngineOpenXIndexCommand(name, keyType, recordType, keyMembers, recordMembers);
        }

        return new StorageEngineOpenXIndexCommand(id);
    }

    private static void WriteMemberMap(BinaryWriter writer, MemberMap? members)
    {
        if (members == null)
        {
            writer.Write((byte)0);
            return;
        }

        writer.Write((byte)1);
        members.Serialize(writer);
    }

    private static MemberMap? ReadMemberMap(BinaryReader reader)
    {
        var marker = reader.ReadByte();
        if (marker == 0) return null;

        return MemberMap.Deserialize(reader);
    }

    private void WriteStorageEngineOpenXFileCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineOpenXFileCommand)command;

        writer.Write(cmd.Name == null);
        if (cmd.Name == null)
            writer.Write(cmd.Id);
        else
            writer.Write(cmd.Name);
    }

    private StorageEngineOpenXFileCommand ReadStorageEngineOpenXFileCommand(BinaryReader reader)
    {
        if (reader.ReadBoolean())
            return new StorageEngineOpenXFileCommand(reader.ReadInt64());
        return new StorageEngineOpenXFileCommand(reader.ReadString());
    }

    private void WriteStorageEngineDeleteCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineDeleteCommand)command;
        writer.Write(cmd.Name);
    }

    private StorageEngineDeleteCommand ReadStorageEngineDeleteCommand(BinaryReader reader)
    {
        return new StorageEngineDeleteCommand(reader.ReadString());
    }

    private void WriteStorageEngineCountCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineCountCommand)command;
        CountCompression.Serialize(writer, (ulong)cmd.Count);
    }

    private StorageEngineCountCommand ReadStorageEngineCountCommand(BinaryReader reader)
    {
        return new StorageEngineCountCommand((int)CountCompression.Deserialize(reader));
    }

    private void WriteStorageEngineFindByNameCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineFindByNameCommand)command;

        writer.Write(cmd.Name ?? "");
        writer.Write(cmd.Descriptor != null);

        if (cmd.Descriptor != null)
            SerializeDescriptor(writer, cmd.Descriptor);
    }

    private StorageEngineFindByNameCommand ReadStorageEngineFindByNameCommand(BinaryReader reader)
    {
        var name = reader.ReadString();
        var description = reader.ReadBoolean() ? DeserializeDescriptor(reader) : null;

        return new StorageEngineFindByNameCommand(name, description);
    }

    private void WriteStorageEngineDescriptionCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineDescriptionCommand)command;
        var description = cmd.Descriptor;

        writer.Write(description != null);

        if (description != null)
            SerializeDescriptor(writer, description);
    }

    private StorageEngineDescriptionCommand ReadStorageEngineDescriptionCommand(BinaryReader reader)
    {
        IDescriptor? description = null;

        if (reader.ReadBoolean()) // Description != null
            description = DeserializeDescriptor(reader);

        return new StorageEngineDescriptionCommand(description);
    }

    private void WriteStorageEngineGetCacheCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineGetCacheSizeCommand)command;

        writer.Write(cmd.CacheSize);
    }

    private StorageEngineGetCacheSizeCommand ReadStorageEngineGetCacheSizeCommand(BinaryReader reader)
    {
        var cacheSize = reader.ReadInt32();

        return new StorageEngineGetCacheSizeCommand(cacheSize);
    }

    private void WriteStorageEngineSetCacheCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (StorageEngineSetCacheSizeCommand)command;

        writer.Write(cmd.CacheSize);
    }

    private StorageEngineSetCacheSizeCommand ReadStorageEngineSetCacheCommand(BinaryReader reader)
    {
        var cacheSize = reader.ReadInt32();

        return new StorageEngineSetCacheSizeCommand(cacheSize);
    }

    #endregion

    #region HeapCommands

    private void WriteHeapObtainNewHandleCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (HeapObtainNewHandleCommand)command;
        writer.Write(cmd.Handle);
    }

    private HeapObtainNewHandleCommand ReadHeapObtainNewHandleCommand(BinaryReader reader)
    {
        return new HeapObtainNewHandleCommand(reader.ReadInt64());
    }

    private void WriteHeapReleaseHandleCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (HeapReleaseHandleCommand)command;
        writer.Write(cmd.Handle);
    }

    private HeapReleaseHandleCommand ReadHeapReleaseHandleCommand(BinaryReader reader)
    {
        return new HeapReleaseHandleCommand(reader.ReadInt64());
    }

    private void WriteHeapExistsHandleCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (HeapExistsHandleCommand)command;
        writer.Write(cmd.Handle);
        writer.Write(cmd.Exist);
    }

    private HeapExistsHandleCommand ReadHeapExistsHandleCommand(BinaryReader reader)
    {
        return new HeapExistsHandleCommand(reader.ReadInt64(), reader.ReadBoolean());
    }

    private void WriteHeapWriteCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (HeapWriteCommand)command;

        writer.Write(cmd.Handle);

        writer.Write(cmd.Count);
        writer.Write(cmd.Index);

        if (cmd.Buffer == null)
            writer.Write(false);
        else
        {
            writer.Write(true);
            writer.Write(cmd.Buffer.Length);
            writer.Write(cmd.Buffer, 0, cmd.Buffer.Length);
        }
    }

    private HeapWriteCommand ReadHeapWriteCommand(BinaryReader reader)
    {
        var handle = reader.ReadInt64();

        var count = reader.ReadInt32();
        var index = reader.ReadInt32();

        byte[]? buffer = null; ;
        if (reader.ReadBoolean())
        {
            buffer = new byte[reader.ReadInt32()];
            reader.Read(buffer, 0, buffer.Length);
        }

        return new HeapWriteCommand(handle, buffer, index, count);
    }

    private void WriteHeapReadCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (HeapReadCommand)command;

        writer.Write(cmd.Handle);

        if (cmd.Buffer == null)
            writer.Write(false);
        else
        {
            writer.Write(true);
            writer.Write(cmd.Buffer.Length);
            writer.Write(cmd.Buffer);
        }
    }

    private HeapReadCommand ReadHeapReadCommand(BinaryReader reader)
    {
        var handle = reader.ReadInt64();

        byte[]? buffer = null;
        if (reader.ReadBoolean())
        {
            var count = reader.ReadInt32();
            buffer = reader.ReadBytes(count);
        }

        return new HeapReadCommand(handle, buffer);
    }

    private void WriteHeapCommitCommand(BinaryWriter writer, ICommand command)
    {
    }

    private HeapCommitCommand ReadHeapCommitCommand(BinaryReader reader)
    {
        return new HeapCommitCommand();
    }

    private void WriteHeapCloseCommand(BinaryWriter writer, ICommand command)
    {
    }

    private HeapCloseCommand ReadHeapCloseCommand(BinaryReader reader)
    {
        return new HeapCloseCommand();
    }

    public void WriteHeapSetTagCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (HeapSetTagCommand)command;

        if (cmd.Buffer == null)
            writer.Write(false);
        else
        {
            writer.Write(true);
            writer.Write(cmd.Buffer.Length);
            writer.Write(cmd.Buffer);
        }
    }

    public HeapSetTagCommand ReadHeapSetTagCommand(BinaryReader reader)
    {
        byte[]? buffer = null;
        if (reader.ReadBoolean())
        {
            var count = reader.ReadInt32();
            buffer = new byte[count];

            reader.Read(buffer, 0, count);
        }

        return new HeapSetTagCommand(buffer);
    }

    public void WriteHeapGetTagCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (HeapGetTagCommand)command;

        if (cmd.Tag == null)
            writer.Write(false);
        else
        {
            writer.Write(true);
            writer.Write(cmd.Tag.Length);
            writer.Write(cmd.Tag);
        }
    }

    public HeapGetTagCommand ReadHeapGetTagCommand(BinaryReader reader)
    {
        byte[]? tag = null;
        if (reader.ReadBoolean())
        {
            var count = reader.ReadInt32();
            tag = new byte[count];

            reader.Read(tag, 0, count);
        }

        return new HeapGetTagCommand(tag);
    }

    public void WriteHeapDataSizeCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (HeapDataSizeCommand)command;

        writer.Write(cmd.DataSize);
    }

    public HeapDataSizeCommand ReadHeapDataSizeCommand(BinaryReader reader)
    {
        return new HeapDataSizeCommand(reader.ReadInt64());
    }

    public void WriteHeapSizeCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (HeapSizeCommand)command;

        writer.Write(cmd.Size);
    }

    public HeapSizeCommand ReadHeapSizeCommand(BinaryReader reader)
    {
        return new HeapSizeCommand(reader.ReadInt64());
    }

    #endregion

    #region Other Commands

    private void WriteExceptionCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (ExceptionCommand)command;
        writer.Write(cmd.Exception);
    }

    private ExceptionCommand ReadExceptionCommand(BinaryReader reader)
    {
        return new ExceptionCommand(reader.ReadString());
    }

    #endregion

    #region Helper Methods

    private void SerializeList(BinaryWriter writer, List<KeyValuePair<IData, IData>> list, int count)
    {
        writer.Write(count);

        foreach (var kv in list)
        {
            KeyPersist.Write(writer, kv.Key);
            RecordPersist.Write(writer, kv.Value);
        }
    }

    private List<KeyValuePair<IData, IData>> DeserializeList(BinaryReader reader)
    {
        var count = reader.ReadInt32();

        var list = new List<KeyValuePair<IData, IData>>(count);
        for (var i = 0; i < count; i++)
        {
            var key = KeyPersist.Read(reader);
            var rec = RecordPersist.Read(reader);

            list.Add(new KeyValuePair<IData, IData>(key, rec));
        }

        return list;
    }

    // ── Index command serialization ───────────────────────────────────────────
    // Field/prefix values are opaque raw bytes (RemoteFieldCodec encodes them with the field type);
    // result lists are (primaryKey, record) pairs encoded with the table's Key/Record persisters.

    private static void WriteBytes(BinaryWriter writer, byte[]? bytes)
    {
        writer.Write(bytes != null);
        if (bytes == null) return;
        writer.Write(bytes.Length);
        writer.Write(bytes);
    }

    private static byte[]? ReadBytes(BinaryReader reader)
    {
        if (!reader.ReadBoolean()) return null;
        var len = reader.ReadInt32();
        return reader.ReadBytes(len);
    }

    private void WriteResults(BinaryWriter writer, List<KeyValuePair<IData, IData>>? results)
    {
        writer.Write(results != null);
        if (results != null)
            SerializeList(writer, results, results.Count);
    }

    private List<KeyValuePair<IData, IData>>? ReadResults(BinaryReader reader)
        => reader.ReadBoolean() ? DeserializeList(reader) : null;

    private static void WriteIndexCreateCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (IndexCreateCommand)command;
        writer.Write(cmd.IndexName);
        writer.Write(cmd.SlotIndices.Length);
        foreach (var s in cmd.SlotIndices) writer.Write(s);
        writer.Write(cmd.MemberNames.Length);
        foreach (var m in cmd.MemberNames) writer.Write(m);
        writer.Write((int)cmd.IndexType);
    }

    private static IndexCreateCommand ReadIndexCreateCommand(BinaryReader reader)
    {
        var name = reader.ReadString();
        var slots = new int[reader.ReadInt32()];
        for (var i = 0; i < slots.Length; i++) slots[i] = reader.ReadInt32();
        var members = new string[reader.ReadInt32()];
        for (var i = 0; i < members.Length; i++) members[i] = reader.ReadString();
        var type = (CatDb.Database.Indexing.IndexType)reader.ReadInt32();
        return new IndexCreateCommand(name, slots, members, type);
    }

    private static void WriteIndexDropCommand(BinaryWriter writer, ICommand command)
        => writer.Write(((IndexDropCommand)command).IndexName);

    private static IndexDropCommand ReadIndexDropCommand(BinaryReader reader)
        => new(reader.ReadString());

    private void WriteIndexFindCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (IndexFindCommand)command;
        writer.Write(cmd.IndexName);
        WriteBytes(writer, cmd.FieldValueRaw);
        WriteResults(writer, cmd.Results);
    }

    private IndexFindCommand ReadIndexFindCommand(BinaryReader reader)
    {
        var name = reader.ReadString();
        var raw = ReadBytes(reader)!;
        return new IndexFindCommand(name, raw) { Results = ReadResults(reader) };
    }

    private void WriteIndexFindRangeCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (IndexFindRangeCommand)command;
        writer.Write(cmd.IndexName);
        WriteBytes(writer, cmd.FromRaw);
        writer.Write(cmd.HasFrom);
        writer.Write(cmd.FromInclusive);
        WriteBytes(writer, cmd.ToRaw);
        writer.Write(cmd.HasTo);
        writer.Write(cmd.ToInclusive);
        writer.Write(cmd.Backward);
        WriteResults(writer, cmd.Results);
    }

    private IndexFindRangeCommand ReadIndexFindRangeCommand(BinaryReader reader)
    {
        var name = reader.ReadString();
        var fromRaw = ReadBytes(reader);
        var hasFrom = reader.ReadBoolean();
        var fromIncl = reader.ReadBoolean();
        var toRaw = ReadBytes(reader);
        var hasTo = reader.ReadBoolean();
        var toIncl = reader.ReadBoolean();
        var backward = reader.ReadBoolean();
        return new IndexFindRangeCommand(name, fromRaw, hasFrom, fromIncl, toRaw, hasTo, toIncl, backward)
        {
            Results = ReadResults(reader)
        };
    }

    private void WriteIndexFindPrefixCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (IndexFindPrefixCommand)command;
        writer.Write(cmd.IndexName);
        WriteBytes(writer, cmd.PrefixRaw);
        writer.Write(cmd.PrefixFieldCount);
        writer.Write(cmd.Backward);
        WriteResults(writer, cmd.Results);
    }

    private IndexFindPrefixCommand ReadIndexFindPrefixCommand(BinaryReader reader)
    {
        var name = reader.ReadString();
        var raw = ReadBytes(reader)!;
        var count = reader.ReadInt32();
        var backward = reader.ReadBoolean();
        return new IndexFindPrefixCommand(name, raw, count, backward) { Results = ReadResults(reader) };
    }

    private static void WriteIndexExistsCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (IndexExistsCommand)command;
        writer.Write(cmd.IndexName);
        WriteBytes(writer, cmd.FieldValueRaw);
        writer.Write(cmd.Result);
    }

    private static IndexExistsCommand ReadIndexExistsCommand(BinaryReader reader)
    {
        var name = reader.ReadString();
        var raw = ReadBytes(reader)!;
        return new IndexExistsCommand(name, raw) { Result = reader.ReadBoolean() };
    }

    private static void WriteIndexCountCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (IndexCountCommand)command;
        writer.Write(cmd.IndexName);
        WriteBytes(writer, cmd.FieldValueRaw);
        writer.Write(cmd.Result);
    }

    private static IndexCountCommand ReadIndexCountCommand(BinaryReader reader)
    {
        var name = reader.ReadString();
        var raw = ReadBytes(reader)!;
        return new IndexCountCommand(name, raw) { Result = reader.ReadInt64() };
    }

    private static void WriteIndexRebuildCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (IndexRebuildCommand)command;
        writer.Write(cmd.IndexName != null);
        if (cmd.IndexName != null) writer.Write(cmd.IndexName);
    }

    private static IndexRebuildCommand ReadIndexRebuildCommand(BinaryReader reader)
        => new(reader.ReadBoolean() ? reader.ReadString() : null);

    private static void WriteIndexListCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (IndexListCommand)command;
        writer.Write(cmd.Results != null);
        if (cmd.Results == null) return;
        writer.Write(cmd.Results.Count);
        foreach (var def in cmd.Results)
        {
            writer.Write(def.Name);
            writer.Write(def.SlotIndices.Length);
            foreach (var s in def.SlotIndices) writer.Write(s);
            writer.Write(def.MemberNames.Length);
            foreach (var m in def.MemberNames) writer.Write(m);
            writer.Write((int)def.Type);
        }
    }

    private static IndexListCommand ReadIndexListCommand(BinaryReader reader)
    {
        var cmd = new IndexListCommand();
        if (!reader.ReadBoolean()) return cmd;
        var count = reader.ReadInt32();
        cmd.Results = new List<CatDb.Database.Indexing.IndexDefinition>(count);
        for (var i = 0; i < count; i++)
        {
            var name = reader.ReadString();
            var slots = new int[reader.ReadInt32()];
            for (var j = 0; j < slots.Length; j++) slots[j] = reader.ReadInt32();
            var members = new string[reader.ReadInt32()];
            for (var j = 0; j < members.Length; j++) members[j] = reader.ReadString();
            var type = (CatDb.Database.Indexing.IndexType)reader.ReadInt32();
            cmd.Results.Add(new CatDb.Database.Indexing.IndexDefinition(name, slots, members, type));
        }
        return cmd;
    }

    private void WriteIndexQueryCommand(BinaryWriter writer, ICommand command)
    {
        var cmd = (IndexQueryCommand)command;

        writer.Write(cmd.Filters.Count);
        foreach (var f in cmd.Filters)
        {
            writer.Write(f.Member);
            writer.Write(f.Op);
            writer.Write(f.FromInclusive);
            writer.Write(f.ToInclusive);
            WriteBytes(writer, f.ValueRaw);
            WriteBytes(writer, f.Value2Raw);
        }

        writer.Write(cmd.Sorts.Count);
        foreach (var s in cmd.Sorts)
        {
            writer.Write(s.Member != null);
            if (s.Member != null) writer.Write(s.Member);
            writer.Write(s.Descending);
        }

        writer.Write(cmd.HasKeyFrom);
        writer.Write(cmd.KeyFromInclusive);
        WriteBytes(writer, cmd.KeyFromRaw);
        writer.Write(cmd.HasKeyTo);
        writer.Write(cmd.KeyToInclusive);
        WriteBytes(writer, cmd.KeyToRaw);

        writer.Write(cmd.Skip);
        writer.Write(cmd.HasTake);
        writer.Write(cmd.Take);

        WriteResults(writer, cmd.Results);
    }

    private IndexQueryCommand ReadIndexQueryCommand(BinaryReader reader)
    {
        var filterCount = reader.ReadInt32();
        var filters = new List<WireFilter>(filterCount);
        for (var i = 0; i < filterCount; i++)
        {
            filters.Add(new WireFilter
            {
                Member = reader.ReadString(),
                Op = reader.ReadByte(),
                FromInclusive = reader.ReadBoolean(),
                ToInclusive = reader.ReadBoolean(),
                ValueRaw = ReadBytes(reader),
                Value2Raw = ReadBytes(reader),
            });
        }

        var sortCount = reader.ReadInt32();
        var sorts = new List<WireSort>(sortCount);
        for (var i = 0; i < sortCount; i++)
        {
            var member = reader.ReadBoolean() ? reader.ReadString() : null;
            sorts.Add(new WireSort { Member = member, Descending = reader.ReadBoolean() });
        }

        var cmd = new IndexQueryCommand(filters, sorts)
        {
            HasKeyFrom = reader.ReadBoolean(),
            KeyFromInclusive = reader.ReadBoolean(),
            KeyFromRaw = ReadBytes(reader),
            HasKeyTo = reader.ReadBoolean(),
            KeyToInclusive = reader.ReadBoolean(),
            KeyToRaw = ReadBytes(reader),
            Skip = reader.ReadInt32(),
            HasTake = reader.ReadBoolean(),
            Take = reader.ReadInt32(),
        };
        cmd.Results = ReadResults(reader);
        return cmd;
    }

    private void SerializeDescriptor(BinaryWriter writer, IDescriptor description)
    {
        CountCompression.Serialize(writer, (ulong)description.Id);
        writer.Write(description.Name ?? string.Empty);

        CountCompression.Serialize(writer, (ulong)description.StructureType);

        description.KeyDataType.Serialize(writer);
        description.RecordDataType.Serialize(writer);

        CountCompression.Serialize(writer, (ulong)description.CreateTime.Ticks);
        CountCompression.Serialize(writer, (ulong)description.ModifiedTime.Ticks);
        CountCompression.Serialize(writer, (ulong)description.AccessTime.Ticks);

        if (description.Tag == null)
            CountCompression.Serialize(writer, 0);
        else
        {
            CountCompression.Serialize(writer, (ulong)description.Tag.Length + 1);
            writer.Write(description.Tag);
        }
    }

    private IDescriptor DeserializeDescriptor(BinaryReader reader)
    {
        var id = (long)CountCompression.Deserialize(reader);
        var name = reader.ReadString();

        var structureType = (int)CountCompression.Deserialize(reader);

        var keyDataType = DataType.Deserialize(reader);
        var recordDataType = DataType.Deserialize(reader);

        var keyType = DataTypeUtils.BuildType(keyDataType);
        var recordType = DataTypeUtils.BuildType(recordDataType);

        var createTime = new DateTime((long)CountCompression.Deserialize(reader));
        var modifiedTime = new DateTime((long)CountCompression.Deserialize(reader));
        var accessTime = new DateTime((long)CountCompression.Deserialize(reader));

        var tagLength = (int)CountCompression.Deserialize(reader) - 1;
        var tag = tagLength >= 0 ? reader.ReadBytes(tagLength) : null;

        return new Descriptor(id, name, structureType, keyDataType, recordDataType, keyType, recordType, createTime, modifiedTime, accessTime, tag);
    }

    #endregion
}
