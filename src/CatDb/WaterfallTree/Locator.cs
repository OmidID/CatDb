// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Diagnostics;
using CatDb.Data;
using CatDb.Database;
using CatDb.General.Collections;
using CatDb.General.Compression;
using CatDb.General.Persist;

namespace CatDb.WaterfallTree;

public class Locator : IDescriptor, IComparable<Locator>, IEquatable<Locator>
    {
        private const byte VERSION = 41;

        private byte[]? _serializationData;

        private readonly int _hashCode;

        private bool _isDeleted;

        private string? _name;

        private Type? _keyType;
        private Type? _recordType;

        private MemberMap? _keyMemberMap;
        private MemberMap? _recordMemberMap;

        private IComparer<IData>? _keyComparer;
        private IEqualityComparer<IData>? _keyEqualityComparer;
        private IPersist<IData>? _keyPersist;
        private IPersist<IData>? _recordPersist;
        private IIndexerPersist<IData>? _keyIndexerPersist;
        private IIndexerPersist<IData>? _recordIndexerPersist;

        private DateTime _createTime;
        private DateTime _modifiedTime;
        private DateTime _accessTime;

        private byte[]? _tag;

        private readonly General.Threading.ReentrantLock _syncRoot = new();

        internal static readonly Locator Min;

        static Locator()
        {
            Min = new Locator(0, null, Database.StructureType.RESERVED, DataType.Boolean, DataType.Boolean, null, null)
                {
                    _keyPersist = SentinelPersistKey.Instance
                };
        }

        public IOperationCollectionFactory OperationCollectionFactory;
        public IOrderedSetFactory OrderedSetFactory;

        /// <summary>
        /// Whether THIS locator's leaves store row data in unmanaged native memory
        /// (<see cref="Database.DatabaseOptions.UseNativeLeafStorage"/>). Set by the owning <see cref="WTree"/>
        /// right after creating/loading the locator — scoped per-tree (NOT a process-wide static), because
        /// multiple <see cref="WTree"/> instances with different settings can be open in the same process
        /// (e.g. multiple engines, or a test suite). A static here previously let one engine's default
        /// (native OFF) options flip the flag for every other already-open native-storage engine mid-run,
        /// corrupting reads with "Native ordered-set image requires UseNativeLeafStorage enabled".
        /// </summary>
        public bool UseNativeLeafStorage;

        public Locator(long id, string? name, int structureType, DataType keyDataType, DataType recordDataType, Type? keyType, Type? recordType)
        {
            if (keyDataType == null)
                throw new ArgumentException("keyDataType");
            if (recordDataType == null)
                throw new ArgumentException("recordDataType");

            Id = id;
            Name = name;
            StructureType = structureType;

            _hashCode = Id.GetHashCode();

            //apply
            Apply = structureType switch
            {
                Database.StructureType.XTABLE => new XTableApply(this),
                Database.StructureType.XFILE => new XStreamApply(this),
                _ => Apply
            };

            KeyDataType = keyDataType;
            RecordDataType = recordDataType;

            KeyType = keyType;
            RecordType = recordType;

            CreateTime = DateTime.Now;
            ModifiedTime = CreateTime;
            AccessTime = CreateTime;

            OperationCollectionFactory = new OperationCollectionFactory(this);
            OrderedSetFactory = new OrderedSetFactory(this);
        }

        private static void WriteMemberMap(BinaryWriter writer, MemberMap? map)
        {
            if (map == null)
            {
                writer.Write(false);
                return;
            }

            writer.Write(true);
            map.Serialize(writer);
        }

        private static MemberMap? ReadMemberMap(BinaryReader reader)
        {
            return reader.ReadBoolean() ? MemberMap.Deserialize(reader) : null;
        }

        private void InternalSerialize(BinaryWriter writer)
        {
            using (_syncRoot.Lock())
            {
                writer.Write(VERSION);

                writer.Write(Id);
                if (Id == Min.Id)
                    return;

                writer.Write(IsDeleted);

                writer.Write(Name!);
                writer.Write(checked((byte)StructureType));

                //data types
                KeyDataType.Serialize(writer);
                RecordDataType.Serialize(writer);

                //types
                if (!DataTypeUtils.IsAnonymousType(KeyType))
                    writer.Write(KeyType!.FullName!);
                else
                    writer.Write("");

                if (!DataTypeUtils.IsAnonymousType(RecordType))
                    writer.Write(RecordType!.FullName!);
                else
                    writer.Write("");

                //key & record members
                WriteMemberMap(writer, _keyMemberMap);
                WriteMemberMap(writer, _recordMemberMap);

                //times
                writer.Write(CreateTime.Ticks);
                writer.Write(ModifiedTime.Ticks);
                writer.Write(AccessTime.Ticks);

                //tag
                if (Tag == null)
                    writer.Write(false);
                else
                {
                    writer.Write(true);
                    CountCompression.Serialize(writer, (ulong)Tag.Length);
                    writer.Write(Tag);
                }
            }
        }

        public void Serialize(BinaryWriter writer)
        {
            using (_syncRoot.Lock())
            {
                if (_serializationData == null)
                {
                    using var ms = new MemoryStream();
                    InternalSerialize(new BinaryWriter(ms));
                    _serializationData = ms.ToArray();
                }

                writer.Write(_serializationData);
            }
        }

        public static Locator Deserialize(BinaryReader reader)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid Locator version.");

            var id = reader.ReadInt64();
            if (id == Min.Id)
                return Min;

            var isDeleted = reader.ReadBoolean();

            var name = reader.ReadString();
            int structureType = reader.ReadByte();

            //data types
            var keyDataType = DataType.Deserialize(reader);
            var recordDataType = DataType.Deserialize(reader);

            //types
            var sKeyType = reader.ReadString();
            var keyType = (sKeyType != "") ? TypeCache.GetType(sKeyType) : DataTypeUtils.BuildType(keyDataType);

            var sRecordType = reader.ReadString();
            var recordType = (sRecordType != "") ? TypeCache.GetType(sRecordType) : DataTypeUtils.BuildType(recordDataType);

            //key & record members
            var keyMemberMap = ReadMemberMap(reader);
            var recordMemberMap = ReadMemberMap(reader);

            //create time
            var createTime = new DateTime(reader.ReadInt64());
            var modifiedTime = new DateTime(reader.ReadInt64());
            var accessTime = new DateTime(reader.ReadInt64());

            //tag
            var tag = reader.ReadBoolean() ? reader.ReadBytes((int)CountCompression.Deserialize(reader)) : null;

            var locator = new Locator(id, name, structureType, keyDataType, recordDataType, keyType, recordType)
                {
                    IsDeleted = isDeleted,
                    _keyMemberMap = keyMemberMap,
                    _recordMemberMap = recordMemberMap,
                    CreateTime = createTime,
                    ModifiedTime = modifiedTime,
                    AccessTime = accessTime,
                    Tag = tag
                };

            return locator;
        }

        public bool IsReady { get; private set; }

        private TypeEngine? _keyEngine;
        private TypeEngine? _recEngine;

        private void DoPrepare()
        {
            Debug.Assert(KeyType != null);
            Debug.Assert(RecordType != null);

            //keys
            if (KeyComparer == null || KeyEqualityComparer == null || (KeyPersist == null || KeyIndexerPersist == null))
            {
                _keyEngine ??= TypeEngine.Default(KeyType);

                KeyComparer ??= _keyEngine.Comparer;

                KeyEqualityComparer ??= _keyEngine.EqualityComparer;

                KeyPersist ??= _keyEngine.Persist;

                KeyIndexerPersist ??= _keyEngine.IndexerPersist;
            }

            //records
            if (RecordPersist == null || RecordIndexerPersist == null)
            {
                _recEngine ??= TypeEngine.Default(RecordType);

                RecordPersist       ??= _recEngine.Persist;
                RecordIndexerPersist ??= _recEngine.IndexerPersist;
            }

            //container
            if (OrderedSetPersist == null)
            {
                if (KeyIndexerPersist != null && RecordIndexerPersist != null)
                    OrderedSetPersist = new OrderedSetPersist(KeyIndexerPersist, RecordIndexerPersist, OrderedSetFactory);
                else
                    OrderedSetPersist = new OrderedSetPersist(KeyPersist, RecordPersist, OrderedSetFactory);
            }

            //operations
            if (OperationsPersist == null)
                OperationsPersist = new OperationCollectionPersist(KeyPersist, RecordPersist, OperationCollectionFactory);

            IsReady = true;
        }

        public IApply? Apply { get; private set; }
        public IPersist<IOrderedSet<IData, IData>>? OrderedSetPersist { get; private set; }
        public IPersist<IOperationCollection>? OperationsPersist { get; private set; }

        public int CompareTo(Locator? other)
        {
            return other is null ? 1 : Id.CompareTo(other.Id);
        }

        public bool Equals(Locator? other)
        {
            return other is not null && Id == other.Id;
        }

        public override bool Equals(object? obj)
        {
            if (obj is not Locator other2)
                return false;

            return Equals(other2);
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }

        public override string ToString()
        {
            return Name ?? "";
        }

        public static bool operator ==(Locator x, Locator y)
        {
            var xNotNull = x is not null;
            var yNotNull = y is not null;

            if (xNotNull && yNotNull)
                return x.Equals(y);

            if (xNotNull || yNotNull)
                return false;

            return true;
        }

        public static bool operator !=(Locator x, Locator y)
        {
            return !(x == y);
        }

        public static implicit operator string(Locator locator)
        {
            return locator.ToString();
        }

        public bool IsDeleted
        {
            get
            {
                using (_syncRoot.Lock())
                    return _isDeleted;
            }

            set
            {
                using (_syncRoot.Lock())
                {
                    if (value != _isDeleted)
                    {
                        _isDeleted = value;
                        _serializationData = null;
                    }
                }
            }
        }

        #region IDescription

        public long Id { get; private set; }

        public string? Name
        {
            get => _name;
            set
            {
                using (_syncRoot.Lock())
                {
                    _name = value;
                    _serializationData = null;
                }
            }
        }

        public int StructureType { get; private set; }

        public DataType KeyDataType { get; private set; }
        public DataType RecordDataType { get; private set; }

        public Type? KeyType
        {
            get => _keyType;
            set
            {
                if (_keyType == value)
                    return;

                if (value != null && DataTypeUtils.BuildDataType(value) != KeyDataType)
                    throw new Exception($"The type {value} is not compatible with anonymous type {KeyDataType}.");
                
                _keyType = value;
                _keyEngine = null;

                _keyComparer = null;
                _keyEqualityComparer = null;
                _keyPersist = null;
                _keyIndexerPersist = null;

                OrderedSetPersist = null;
                OperationsPersist = null;

                IsReady = false;
            }
        }

        public Type? RecordType
        {
            get => _recordType;
            set
            {
                if (_recordType == value)
                    return;

                if (value != null && DataTypeUtils.BuildDataType(value) != RecordDataType)
                    throw new Exception($"The type {value} is not compatible with anonymous type {RecordDataType}.");

                _recordType = value;
                _recEngine = null;

                _recordPersist = null;
                _recordIndexerPersist = null;

                OrderedSetPersist = null;
                OperationsPersist = null;

                IsReady = false;
            }
        }

        public IComparer<IData>? KeyComparer
        {
            get
            { 
                using (_syncRoot.Lock())
                    return _keyComparer; 
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _keyComparer = value;
                    IsReady = false;
                }
            }
        }

        public IEqualityComparer<IData>? KeyEqualityComparer
        {
            get
            { 
                using (_syncRoot.Lock())
                    return _keyEqualityComparer; 
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _keyEqualityComparer = value;
                    IsReady = false;
                }
            }
        }

        public IPersist<IData>? KeyPersist
        {
            get
            {
                using (_syncRoot.Lock())
                    return _keyPersist; 
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _keyPersist = value;

                    OrderedSetPersist = null;
                    OperationsPersist = null;

                    IsReady = false;
                }
            }
        }

        public IPersist<IData>? RecordPersist
        {
            get
            { 
                using (_syncRoot.Lock())
                    return _recordPersist; 
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _recordPersist = value;

                    OrderedSetPersist = null;
                    OperationsPersist = null;

                    IsReady = false;
                }
            }
        }

        public IIndexerPersist<IData>? KeyIndexerPersist
        {
            get
            { 
                using (_syncRoot.Lock())
                    return _keyIndexerPersist; 
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _keyIndexerPersist = value;
                    OrderedSetPersist = null;

                    IsReady = false;
                }
            }
        }

        public IIndexerPersist<IData>? RecordIndexerPersist
        {
            get
            {
                using (_syncRoot.Lock())
                    return _recordIndexerPersist; 
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _recordIndexerPersist = value;
                    OrderedSetPersist = null;

                    IsReady = false;
                }
            }
        }

        public void Prepare()
        {
            if (!IsReady)
            {
                using (_syncRoot.Lock())
                    DoPrepare();
            }
        }

        public DateTime CreateTime
        {
            get
            {
                using (_syncRoot.Lock())
                    return _createTime;
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _createTime = value;
                    _serializationData = null;
                }
            }
        }

        public DateTime ModifiedTime
        {
            get
            {
                using (_syncRoot.Lock())
                    return _modifiedTime;
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _modifiedTime = value;
                    _serializationData = null;
                }
            }
        }

        public DateTime AccessTime
        {
            get
            {
                using (_syncRoot.Lock())
                    return _accessTime;
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _accessTime = value;
                    _serializationData = null;
                }
            }
        }

        public byte[]? Tag
        {
            get
            { 
                using (_syncRoot.Lock())
                    return _tag; 
            }
            set
            {
                using (_syncRoot.Lock())
                {
                    _tag = value;
                    _serializationData = null;
                }
            }
        }

        public IReadOnlyDictionary<string, int>? KeyMembers
        {
            get
            {
                using (_syncRoot.Lock())
                    return _keyMemberMap?.Names;
            }
        }

        public IReadOnlyDictionary<string, int>? RecordMembers
        {
            get
            {
                using (_syncRoot.Lock())
                    return _recordMemberMap?.Names;
            }
        }

        public MemberMap? KeyMemberMap
        {
            get
            {
                using (_syncRoot.Lock())
                    return _keyMemberMap;
            }
        }

        public MemberMap? RecordMemberMap
        {
            get
            {
                using (_syncRoot.Lock())
                    return _recordMemberMap;
            }
        }

        /// <summary>
        /// Captures the public member names from a concrete (non-anonymous) type
        /// and stores them as the key/record member map.  This is persisted with
        /// the locator so any future opener (including the HTTP API) can map
        /// slot indices → human-readable field names.
        /// </summary>
        internal void CaptureMembers(Type? keyType, Type? recordType)
        {
            using (_syncRoot.Lock())
            {
                if (_keyMemberMap == null && keyType != null && !DataTypeUtils.IsAnonymousType(keyType))
                {
                    _keyMemberMap = BuildMemberMap(KeyDataType, keyType);
                    _serializationData = null;
                }

                if (_recordMemberMap == null && recordType != null && !DataTypeUtils.IsAnonymousType(recordType))
                {
                    _recordMemberMap = BuildMemberMap(RecordDataType, recordType);
                    _serializationData = null;
                }
            }
        }

        /// <summary>
        /// Stores explicitly provided recursive member maps (sent by a remote client
        /// or the HTTP schema layer that knows the real field names — including
        /// nested object/collection members).  Only sets if not already populated.
        /// </summary>
        internal void SetMembers(MemberMap? keyMemberMap, MemberMap? recordMemberMap)
        {
            using (_syncRoot.Lock())
            {
                if (_keyMemberMap == null && keyMemberMap != null)
                {
                    _keyMemberMap = keyMemberMap;
                    _serializationData = null;
                }

                if (_recordMemberMap == null && recordMemberMap != null)
                {
                    _recordMemberMap = recordMemberMap;
                    _serializationData = null;
                }
            }
        }

        /// <summary>
        /// Stores flat (top-level only) member maps. Kept for callers that pass a
        /// simple name→index dictionary; wrapped into a childless <see cref="MemberMap"/>.
        /// </summary>
        internal void SetMembers(Dictionary<string, int>? keyMembers, Dictionary<string, int>? recordMembers)
        {
            SetMembers(
                keyMembers is { Count: > 0 } ? new MemberMap(keyMembers) : null,
                recordMembers is { Count: > 0 } ? new MemberMap(recordMembers) : null);
        }

        private static MemberMap BuildMemberMap(DataType dataType, Type type)
        {
            if (DataType.IsPrimitiveType(type))
                return new MemberMap(new Dictionary<string, int> { [type.Name] = 0 });

            return MemberMap.Build(dataType, type)
                   ?? new MemberMap(new Dictionary<string, int>());
        }

        #endregion
    }
