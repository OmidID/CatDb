using System.Diagnostics;
using CatDb.Data;
using CatDb.Database.Operations;
using CatDb.General.Collections;
using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public class XStreamApply : IApply
    {
        const int BLOCK_SIZE = XStream.BLOCK_SIZE;

        public XStreamApply(Locator path)
        {
            Locator = path;
        }

        public bool Internal(IOperationCollection operations)
        {
            return false;
        }

        public bool Leaf(IOperationCollection operations, IOrderedSet<IData, IData> data)
        {
            var isModified = false;

            foreach (var opr in operations)
            {
                switch (opr.Code)
                {
                    case OperationCode.REPLACE:
                        {
                            if (Replace(data, (ReplaceOperation)opr))
                                isModified = true;
                        }
                        break;
                    case OperationCode.DELETE:
                        {
                            var o = new DeleteRangeOperation(opr.FromKey, opr.FromKey);
                            if (Delete(data, o))
                                isModified = true;
                        }
                        break;
                    case OperationCode.DELETE_RANGE:
                        {
                            if (Delete(data, (DeleteRangeOperation)opr))
                                isModified = true;
                        }
                        break;

                    default:
                        throw new NotSupportedException();
                }
            }
            return isModified;
        }

        public Locator Locator { get; private set; }

        private bool Replace(IOrderedSet<IData, IData> set, ReplaceOperation operation)
        {
            Debug.Assert(operation.Scope == OperationScope.Point);

            var from = ((Data<long>)operation.FromKey).Value;
            var localFrom = (int)(from % BLOCK_SIZE);
            var baseFrom = from - localFrom;
            var baseKey = new Data<long>(baseFrom);

            var src = ((Data<byte[]>)operation.Record).Value;
            Debug.Assert(src.Length <= BLOCK_SIZE);
            Debug.Assert(baseFrom == BLOCK_SIZE * ((from + src.Length - 1) / BLOCK_SIZE));

            if (set.TryGetValue(baseKey, out var tmp))
            {
                var rec = (Data<byte[]>)tmp;

                if (localFrom == 0 && src.Length >= rec.Value.Length)
                    rec.Value = src;
                else
                {
                    Debug.Assert(src.Length < BLOCK_SIZE);
                    var dst = rec.Value;
                    if (dst.Length > localFrom + src.Length)
                        src.CopyTo(dst, localFrom);
                    else
                    {
                        var buffer = new byte[localFrom + src.Length];
                        dst.CopyTo(buffer, 0);
                        src.CopyTo(buffer, localFrom);
                        rec.Value = buffer;
                    }
                }
            }
            else // if element with baseKey is not found
            {
                if (localFrom == 0)
                    set[baseKey] = new Data<byte[]>(src);
                else
                {
                    var values = new byte[localFrom + src.Length];
                    src.CopyTo(values, localFrom);
                    set[baseKey] = new Data<byte[]>(values);
                }
            }

            return true;
        }

        private bool Delete(IOrderedSet<IData, IData> set, DeleteRangeOperation operation)
        {
            var from = ((Data<long>)operation.FromKey).Value;
            var to = ((Data<long>)operation.ToKey).Value;

            var localFrom = (int)(from % BLOCK_SIZE);
            var localTo = (int)(to % BLOCK_SIZE);
            var baseFrom = from - localFrom;
            var baseTo = to - localTo;

            var internalFrom = localFrom > 0 ? baseFrom + BLOCK_SIZE : baseFrom;
            var internalTo = localTo < BLOCK_SIZE - 1 ? baseTo - 1 : baseTo;

            var isModified = false;

            if (internalFrom <= internalTo)
                isModified = set.Remove(new Data<long>(internalFrom), true, new Data<long>(internalTo), true);

            Data<byte[]> record;

            if (localFrom > 0 && set.TryGetValue(new Data<long>(baseFrom), out var tmp))
            {
                record = (Data<byte[]>)tmp;
                if (localFrom < record.Value.Length)
                {
                    Array.Clear(record.Value, localFrom, baseFrom < baseTo ? record.Value.Length - localFrom : localTo - localFrom + 1);
                    isModified = true;
                }
                if (baseFrom == baseTo)
                    return isModified;
            }

            if (localTo < BLOCK_SIZE - 1 && set.TryGetValue(new Data<long>(baseTo), out tmp))
            {
                record = (Data<byte[]>)tmp;
                if (localTo < record.Value.Length - 1)
                {
                    Array.Clear(record.Value, 0, localTo + 1);
                    isModified = true;
                }
                else
                    isModified = set.Remove(new Data<long>(baseTo));
            }

            return isModified;
        }
    }
}
