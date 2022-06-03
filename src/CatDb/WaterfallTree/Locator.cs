using System.Diagnostics;
using CatDb.Data;
using CatDb.Database;
using CatDb.General.Collections;
using CatDb.General.Compression;
using CatDb.General.Persist;

namespace CatDb.WaterfallTree
{
    public class Locator : IDescriptor, IComparable<Locator>, IEquatable<Locator>
    {
        private const byte VERSION = 40;

        private byte[] _serializationData;

        private readonly int _hashCode;

        private bool _isDeleted;

        private string _name;

        private Type _keyType;
        private Type _recordType;

        private Dictionary<string, int> _keyMembers;
        private Dictionary<string, int> _recordMembers;

        private IComparer<IData> _keyComparer;
        private IEqualityComparer<IData> _keyEqualityComparer;
        private IPersist<IData> _keyPersist;
        private IPersist<IData> _recordPersist;
        private IIndexerPersist<IData> _keyIndexerPersist;
        private IIndexerPersist<IData> _recordIndexerPersist;

        private DateTime _createTime;
        private DateTime _modifiedTime;
        private DateTime _accessTime;

        private byte[] _tag;

        private readonly object _syncRoot = new();

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

        public Locator(long id, string name, int structureType, DataType keyDataType, DataType recordDataType, Type keyType, Type recordType)
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

        private void WriteMembers(BinaryWriter writer, Dictionary<string, int> members)
        {
            if (members == null)
            {
                writer.Write(false);
                return;
            }

            writer.Write(true);
            writer.Write(members.Count);

            foreach (var kv in members)
            {
                writer.Write(kv.Key);
                writer.Write(kv.Value);
            }
        }

        private static Dictionary<string, int> ReadMembers(BinaryReader reader)
        {
            if (!reader.ReadBoolean())
                return null;

            var count = reader.ReadInt32();
            var members = new Dictionary<string, int>(count);

            for (var i = 0; i < count; i++)
            {
                var key = reader.ReadString();
                var value = reader.ReadInt32();

                members.Add(key, value);
            }

            return members;
        }

        private void InternalSerialize(BinaryWriter writer)
        {
            lock (_syncRoot)
            {
                writer.Write(VERSION);

                writer.Write(Id);
                if (Id == Min.Id)
                    return;

                writer.Write(IsDeleted);

                writer.Write(Name);
                writer.Write(checked((byte)StructureType));

                //data types
                KeyDataType.Serialize(writer);
                RecordDataType.Serialize(writer);

                //types
                if (!DataTypeUtils.IsAnonymousType(KeyType))
                    writer.Write(KeyType.FullName);
                else
                    writer.Write("");

                if (!DataTypeUtils.IsAnonymousType(RecordType))
                    writer.Write(RecordType.FullName);
                else
                    writer.Write("");

                //key & record members
                WriteMembers(writer, _keyMembers);
                WriteMembers(writer, _recordMembers);

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
            lock (_syncRoot)
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
            var keyMembers = ReadMembers(reader);
            var recordMembers = ReadMembers(reader);

            //create time
            var createTime = new DateTime(reader.ReadInt64());
            var modifiedTime = new DateTime(reader.ReadInt64());
            var accessTime = new DateTime(reader.ReadInt64());

            //tag
            var tag = reader.ReadBoolean() ? reader.ReadBytes((int)CountCompression.Deserialize(reader)) : null;

            var locator = new Locator(id, name, structureType, keyDataType, recordDataType, keyType, recordType)
                {
                    IsDeleted = isDeleted,
                    _keyMembers = keyMembers,
                    _recordMembers = recordMembers,
                    CreateTime = createTime,
                    ModifiedTime = modifiedTime,
                    AccessTime = accessTime,
                    Tag = tag
                };

            return locator;
        }

        public bool IsReady { get; private set; }

        private TypeEngine _keyEngine;
        private TypeEngine _recEngine;

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
                if (_recEngine == null)
                    _recEngine = TypeEngine.Default(RecordType);

                if (RecordPersist == null)
                    RecordPersist = _recEngine.Persist;

                if (RecordIndexerPersist == null)
                    RecordIndexerPersist = _recEngine.IndexerPersist;
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

        public IApply Apply { get; private set; }
        public IPersist<IOrderedSet<IData, IData>> OrderedSetPersist { get; private set; }
        public IPersist<IOperationCollection> OperationsPersist { get; private set; }

        public int CompareTo(Locator other)
        {
            return Id.CompareTo(other.Id);
        }

        public bool Equals(Locator other)
        {
            return Id == other.Id;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is Locator))
                return false;

            return Equals((Locator)obj);
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }

        public override string ToString()
        {
            return Name;
        }

        public static bool operator ==(Locator x, Locator y)
        {
            var xNotNull = !ReferenceEquals(x, null);
            var yNotNull = !ReferenceEquals(y, null);

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
                lock (_syncRoot)
                    return _isDeleted;
            }

            set
            {
                lock (_syncRoot)
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

        public string Name
        {
            get => _name;
            set
            {
                lock (_syncRoot)
                {
                    _name = value;
                    _serializationData = null;
                }
            }
        }

        public int StructureType { get; private set; }

        public DataType KeyDataType { get; private set; }
        public DataType RecordDataType { get; private set; }

        public Type KeyType
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

        public Type RecordType
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

        public IComparer<IData> KeyComparer
        {
            get
            { 
                lock (_syncRoot)
                    return _keyComparer; 
            }
            set
            {
                lock (_syncRoot)
                {
                    _keyComparer = value;
                    IsReady = false;
                }
            }
        }

        public IEqualityComparer<IData> KeyEqualityComparer
        {
            get
            { 
                lock (_syncRoot)
                    return _keyEqualityComparer; 
            }
            set
            {
                lock (_syncRoot)
                {
                    _keyEqualityComparer = value;
                    IsReady = false;
                }
            }
        }

        public IPersist<IData> KeyPersist
        {
            get
            {
                lock (_syncRoot)
                    return _keyPersist; 
            }
            set
            {
                lock (_syncRoot)
                {
                    _keyPersist = value;

                    OrderedSetPersist = null;
                    OperationsPersist = null;

                    IsReady = false;
                }
            }
        }

        public IPersist<IData> RecordPersist
        {
            get
            { 
                lock (_syncRoot)
                    return _recordPersist; 
            }
            set
            {
                lock (_syncRoot)
                {
                    _recordPersist = value;

                    OrderedSetPersist = null;
                    OperationsPersist = null;

                    IsReady = false;
                }
            }
        }

        public IIndexerPersist<IData> KeyIndexerPersist
        {
            get
            { 
                lock (_syncRoot)
                    return _keyIndexerPersist; 
            }
            set
            {
                lock (_syncRoot)
                {
                    _keyIndexerPersist = value;
                    OrderedSetPersist = null;

                    IsReady = false;
                }
            }
        }

        public IIndexerPersist<IData> RecordIndexerPersist
        {
            get
            {
                lock (_syncRoot)
                    return _recordIndexerPersist; 
            }
            set
            {
                lock (_syncRoot)
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
                lock (_syncRoot)
                    DoPrepare();
            }
        }

        public DateTime CreateTime
        {
            get
            {
                lock (_syncRoot)
                    return _createTime;
            }
            set
            {
                lock (_syncRoot)
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
                lock (_syncRoot)
                    return _modifiedTime;
            }
            set
            {
                lock (_syncRoot)
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
                lock (_syncRoot)
                    return _accessTime;
            }
            set
            {
                lock (_syncRoot)
                {
                    _accessTime = value;
                    _serializationData = null;
                }
            }
        }

        public byte[] Tag
        {
            get
            { 
                lock (_syncRoot)
                    return _tag; 
            }
            set
            {
                lock (_syncRoot)
                {
                    _tag = value;
                    _serializationData = null;
                }
            }
        }

        #endregion
    }
}
