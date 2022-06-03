using CatDb.Data;
using CatDb.Database.Operations;
using CatDb.General.Persist;
using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public class OperationCollectionPersist : IPersist<IOperationCollection>
    {
        private const byte VERSION = 40;

        private readonly Action<BinaryWriter, IOperation>[] _writes;
        private readonly Func<BinaryReader, IOperation>[] _reads;

        private readonly IPersist<IData> _keyPersist;
        private readonly IPersist<IData> _recordPersist;
        private readonly IOperationCollectionFactory _collectionFactory;

        public OperationCollectionPersist(IPersist<IData> keyPersist, IPersist<IData> recordPersist, IOperationCollectionFactory collectionFactory)
        {
            _keyPersist = keyPersist;
            _recordPersist = recordPersist;

            _writes = new Action<BinaryWriter, IOperation>[OperationCode.MAX];
            _writes[OperationCode.REPLACE] = WriteReplaceOperation;
            _writes[OperationCode.INSERT_OR_IGNORE] = WriteInsertOrIgnoreOperation;
            _writes[OperationCode.DELETE] = WriteDeleteOperation;
            _writes[OperationCode.DELETE_RANGE] = WriteDeleteRangeOperation;
            _writes[OperationCode.CLEAR] = WriteClearOperation;

            _reads = new Func<BinaryReader, IOperation>[OperationCode.MAX];
            _reads[OperationCode.REPLACE] = ReadReplaceOperation;
            _reads[OperationCode.INSERT_OR_IGNORE] = ReadInsertOrIgnoreOperation;
            _reads[OperationCode.DELETE] = ReadDeleteOperation;
            _reads[OperationCode.DELETE_RANGE] = ReadDeleteRangeOperation;
            _reads[OperationCode.CLEAR] = ReadClearOperation;

            _collectionFactory = collectionFactory;
        }

        #region Write Methods

        private void WriteReplaceOperation(BinaryWriter writer, IOperation operation)
        {
            var opr = (ReplaceOperation)operation;

            _keyPersist.Write(writer, opr.FromKey);
            _recordPersist.Write(writer, opr.Record);
        }

        private void WriteInsertOrIgnoreOperation(BinaryWriter writer, IOperation operation)
        {
            var opr = (InsertOrIgnoreOperation)operation;

            _keyPersist.Write(writer, opr.FromKey);
            _recordPersist.Write(writer, opr.Record);
        }

        private void WriteDeleteOperation(BinaryWriter writer, IOperation operation)
        {
            _keyPersist.Write(writer, operation.FromKey);
        }

        private void WriteDeleteRangeOperation(BinaryWriter writer, IOperation operation)
        {
            _keyPersist.Write(writer, operation.FromKey);
            _keyPersist.Write(writer, operation.ToKey);
        }

        private void WriteClearOperation(BinaryWriter writer, IOperation operation)
        {
            //do nothing
        }

        #endregion

        #region Read Methods

        private IOperation ReadReplaceOperation(BinaryReader reader)
        {
            var key = _keyPersist.Read(reader);
            var record = _recordPersist.Read(reader);

            return new ReplaceOperation(key, record);
        }

        private IOperation ReadInsertOrIgnoreOperation(BinaryReader reader)
        {
            var key = _keyPersist.Read(reader);
            var record = _recordPersist.Read(reader);

            return new InsertOrIgnoreOperation(key, record);
        }

        private IOperation ReadDeleteOperation(BinaryReader reader)
        {
            var key = _keyPersist.Read(reader);

            return new DeleteOperation(key);
        }

        private IOperation ReadDeleteRangeOperation(BinaryReader reader)
        {
            var from = _keyPersist.Read(reader);
            var to = _keyPersist.Read(reader);

            return new DeleteRangeOperation(from, to);
        }

        private IOperation ReadClearOperation(BinaryReader reader)
        {
            return new ClearOperation();
        }

        #endregion

        public void Write(BinaryWriter writer, IOperationCollection item)
        {
            writer.Write(VERSION);
            
            writer.Write(item.Count);
            writer.Write(item.AreAllMonotoneAndPoint);

            var commonAction = item.CommonAction;
            writer.Write(commonAction);

            if (commonAction > 0)
            {
                switch (commonAction)
                {
                    case OperationCode.REPLACE:
                        {
                            for (var i = 0; i < item.Count; i++)
                                WriteReplaceOperation(writer, item[i]);
                        }
                        break;

                    case OperationCode.INSERT_OR_IGNORE:
                        {
                            for (var i = 0; i < item.Count; i++)
                                WriteInsertOrIgnoreOperation(writer, item[i]);
                        }
                        break;

                    case OperationCode.DELETE:
                        {
                            for (var i = 0; i < item.Count; i++)
                                WriteDeleteOperation(writer, item[i]);
                        }
                        break;

                    case OperationCode.DELETE_RANGE:
                        {
                            for (var i = 0; i < item.Count; i++)
                                WriteDeleteRangeOperation(writer, item[i]);
                        }
                        break;

                    case OperationCode.CLEAR:
                        {
                            for (var i = 0; i < item.Count; i++)
                                WriteClearOperation(writer, item[i]);
                        }
                        break;

                    default:
                        throw new NotSupportedException(commonAction.ToString());
                }
            }
            else
            {
                for (var i = 0; i < item.Count; i++)
                {
                    var operation = item[i];
                    writer.Write(operation.Code);
                    _writes[operation.Code](writer, operation);
                }
            }
        }

        public IOperationCollection Read(BinaryReader reader)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid OperationCollectionPersist version.");
            
            var count = reader.ReadInt32();
            var areAllMonotoneAndPoint = reader.ReadBoolean();
            var commonAction = reader.ReadInt32();

            var array = new IOperation[count];

            if (commonAction > 0)
            {
                switch (commonAction)
                {
                    case OperationCode.REPLACE:
                        {
                            for (var i = 0; i < count; i++)
                                array[i] = ReadReplaceOperation(reader);
                        }
                        break;

                    case OperationCode.INSERT_OR_IGNORE:
                        {
                            for (var i = 0; i < count; i++)
                                array[i] = ReadInsertOrIgnoreOperation(reader);
                        }
                        break;

                    case OperationCode.DELETE:
                        {
                            for (var i = 0; i < count; i++)
                                array[i] = ReadDeleteOperation(reader);
                        }
                        break;

                    case OperationCode.DELETE_RANGE:
                        {
                            for (var i = 0; i < count; i++)
                                array[i] = ReadDeleteRangeOperation(reader);
                        }
                        break;

                    case OperationCode.CLEAR:
                        {
                            for (var i = 0; i < count; i++)
                                array[i] = ReadClearOperation(reader);
                        }
                        break;

                    default:
                        throw new NotSupportedException(commonAction.ToString());
                }
            }
            else
            {
                for (var i = 0; i < count; i++)
                {
                    var code = reader.ReadInt32();
                    array[i] = _reads[code](reader);
                }
            }

            var operations = _collectionFactory.Create(array, commonAction, areAllMonotoneAndPoint);

            return operations;
        }
    }
}