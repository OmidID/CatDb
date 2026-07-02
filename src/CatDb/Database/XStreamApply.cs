// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Diagnostics;
using CatDb.Database.Operations;
using CatDb.General.Collections;
using CatDb.WaterfallTree;

namespace CatDb.Database;

// IData = object. Keys are boxed long; records are byte[] stored directly as object.
public class XStreamApply(Locator path) : IApply
{
    private const int BLOCK_SIZE = XStream.BLOCK_SIZE;

    public Locator Locator { get; } = path;

    public bool Internal(IOperationCollection operations) => false;

    public bool Leaf(IOperationCollection operations, IOrderedSet<IData, IData> data)
    {
        var isModified = false;

        foreach (var opr in operations)
        {
            switch (opr.Code)
            {
                case OperationCode.REPLACE:
                    if (Replace(data, (ReplaceOperation)opr))
                        isModified = true;
                    break;

                case OperationCode.DELETE:
                    if (Delete(data, new DeleteRangeOperation(opr.FromKey, opr.FromKey)))
                        isModified = true;
                    break;

                case OperationCode.DELETE_RANGE:
                    if (Delete(data, (DeleteRangeOperation)opr))
                        isModified = true;
                    break;

                default:
                    throw new NotSupportedException();
            }
        }

        return isModified;
    }

    private bool Replace(IOrderedSet<IData, IData> set, ReplaceOperation operation)
    {
        Debug.Assert(operation.Scope == OperationScope.Point);

        var from      = (long)operation.FromKey;
        var localFrom = (int)(from % BLOCK_SIZE);
        var baseFrom  = from - localFrom;
        IData baseKey = (object)baseFrom;

        var src = (byte[])operation.Record;
        Debug.Assert(src.Length <= BLOCK_SIZE);
        Debug.Assert(baseFrom == BLOCK_SIZE * ((from + src.Length - 1) / BLOCK_SIZE));

        if (set.TryGetValue(baseKey, out var tmp))
        {
            var dst = (byte[])tmp;

            if (localFrom == 0 && src.Length >= dst.Length)
            {
                set[baseKey] = (object)src;
            }
            else
            {
                Debug.Assert(src.Length < BLOCK_SIZE);
                if (dst.Length > localFrom + src.Length)
                {
                    src.CopyTo(dst, localFrom); // mutate in-place — same array object already in set
                }
                else
                {
                    var buffer = new byte[localFrom + src.Length];
                    dst.CopyTo(buffer, 0);
                    src.CopyTo(buffer, localFrom);
                    set[baseKey] = (object)buffer;
                }
            }
        }
        else
        {
            if (localFrom == 0)
            {
                set[baseKey] = (object)src;
            }
            else
            {
                var values = new byte[localFrom + src.Length];
                src.CopyTo(values, localFrom);
                set[baseKey] = (object)values;
            }
        }

        return true;
    }

    private bool Delete(IOrderedSet<IData, IData> set, DeleteRangeOperation operation)
    {
        var from = (long)operation.FromKey;
        var to   = (long)operation.ToKey;

        var localFrom = (int)(from % BLOCK_SIZE);
        var localTo   = (int)(to   % BLOCK_SIZE);
        var baseFrom  = from - localFrom;
        var baseTo    = to   - localTo;

        var internalFrom = localFrom > 0             ? baseFrom + BLOCK_SIZE : baseFrom;
        var internalTo   = localTo  < BLOCK_SIZE - 1 ? baseTo  - 1          : baseTo;

        var isModified = false;

        if (internalFrom <= internalTo)
            isModified = set.Remove((object)internalFrom, true, (object)internalTo, true);

        if (localFrom > 0 && set.TryGetValue((object)baseFrom, out var tmp))
        {
            var record = (byte[])tmp;
            if (localFrom < record.Length)
            {
                Array.Clear(record, localFrom, baseFrom < baseTo ? record.Length - localFrom : localTo - localFrom + 1);
                isModified = true;
            }
            if (baseFrom == baseTo)
                return isModified;
        }

        if (localTo < BLOCK_SIZE - 1 && set.TryGetValue((object)baseTo, out tmp))
        {
            var record = (byte[])tmp;
            if (localTo < record.Length - 1)
            {
                Array.Clear(record, 0, localTo + 1);
                isModified = true;
            }
            else
                isModified = set.Remove((object)baseTo);
        }

        return isModified;
    }
}
