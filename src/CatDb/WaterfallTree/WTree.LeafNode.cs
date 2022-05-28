using System.Diagnostics;
using CatDb.General.Compression;
using CatDb.General.Collections;
using CatDb.Data;

namespace CatDb.WaterfallTree
{
    public partial class WTree
    {
        private sealed class LeafNode : Node
        {
            public const byte VERSION = 40;

            /// <summary>
            /// Total number of records in the node
            /// </summary>
            public int RecordCount { get; private set; }

            public readonly Dictionary<Locator, IOrderedSet<IData, IData>> Container;

            public LeafNode(Branch branch, bool isModified)
                : base(branch)
            {
                Debug.Assert(branch.NodeType == NodeType.Leaf);

                Container = new Dictionary<Locator, IOrderedSet<IData, IData>>();
                IsModified = isModified;
            }

            public override void Apply(IOperationCollection operations)
            {
                var locator = operations.Locator;

                IOrderedSet<IData, IData> data;
                if (Container.TryGetValue(locator, out data))
                {
                    RecordCount -= data.Count;

                    if (locator.Apply.Leaf(operations, data))
                        IsModified = true;

                    RecordCount += data.Count;

                    if (data.Count == 0)
                        Container.Remove(locator);
                }
                else
                {
                    data = locator.OrderedSetFactory.Create();
                    Debug.Assert(data != null);
                    if (locator.Apply.Leaf(operations, data))
                        IsModified = true;

                    RecordCount += data.Count;

                    if (data.Count > 0)
                        Container.Add(locator, data);
                }
            }

            public override Node Split()
            {
                var HALF_RECORD_COUNT = RecordCount / 2;

                var rightBranch = new Branch(Branch.Tree, NodeType.Leaf);
                var rightNode = ((LeafNode)rightBranch.Node);
                var rightContainer = rightNode.Container;

                var leftRecordCount = 0;

                var specialCase = new KeyValuePair<Locator, IOrderedSet<IData, IData>>(default(Locator), null);

                if (Container.Count == 1)
                {
                    var kv = Container.First();
                    var data = kv.Value.Split(HALF_RECORD_COUNT);

                    Debug.Assert(data.Count > 0);
                    rightContainer.Add(kv.Key, data);
                    leftRecordCount = RecordCount - data.Count;
                }
                else //if (Container.Count > 1)
                {
                    var enumerator = Container.OrderBy(x => x.Key).GetEnumerator();

                    var emptyContainers = new List<Locator>();

                    //the left part
                    while (enumerator.MoveNext())
                    {
                        var kv = enumerator.Current;
                        if (kv.Value.Count == 0)
                        {
                            emptyContainers.Add(kv.Key);
                            continue;
                        }

                        leftRecordCount += kv.Value.Count;
                        if (leftRecordCount < HALF_RECORD_COUNT)
                            continue;

                        if (leftRecordCount > HALF_RECORD_COUNT)
                        {
                            var data = kv.Value.Split(leftRecordCount - HALF_RECORD_COUNT);
                            if (data.Count > 0)
                            {
                                specialCase = new KeyValuePair<Locator, IOrderedSet<IData, IData>>(kv.Key, data);
                                leftRecordCount -= data.Count;
                            }
                        }

                        break;
                    }

                    //the right part
                    while (enumerator.MoveNext())
                    {
                        var kv = enumerator.Current;
                        if (kv.Value.Count == 0)
                        {
                            emptyContainers.Add(kv.Key);
                            continue;
                        }

                        rightContainer[kv.Key] = kv.Value;
                    }

                    foreach (var kv in rightContainer)
                        Container.Remove(kv.Key);

                    foreach (var key in emptyContainers)
                        Container.Remove(key);

                    if (specialCase.Value != null) //have special case?
                        rightContainer[specialCase.Key] = specialCase.Value;
                }

                rightNode.RecordCount = RecordCount - leftRecordCount;
                RecordCount = leftRecordCount;
                rightNode.TouchID = TouchID;
                IsModified = true;

                return rightNode;
            }

            public override void Merge(Node node)
            {
                foreach (var kv in ((LeafNode)node).Container)
                {
                    IOrderedSet<IData, IData> data;
                    if (!Container.TryGetValue(kv.Key, out data))
                        Container[kv.Key] = data = kv.Value;
                    else
                    {
                        RecordCount -= data.Count;
                        data.Merge(kv.Value);
                    }

                    RecordCount += data.Count;
                }

                if (TouchID < node.TouchID)
                    TouchID = node.TouchID;

                IsModified = true;
            }

            public override bool IsOverflow => RecordCount > Branch.Tree.LEAF_NODE_MAX_RECORDS;

            public override bool IsUnderflow
            {
                get
                {
                    if (IsRoot)
                        return false;

                    return RecordCount < Branch.Tree.LEAF_NODE_MIN_RECORDS;
                }
            }

            public override FullKey FirstKey
            {
                get
                {
                    var kv = (Container.Count == 1) ? Container.First() : Container.OrderBy(x => x.Key).First();

                    return new FullKey(kv.Key, kv.Value.First.Key);
                }
            }

            public override void Store(Stream stream)
            {
                var writer = new BinaryWriter(stream);
                writer.Write(VERSION);

                CountCompression.Serialize(writer, checked((ulong)Branch.NodeHandle));

                CountCompression.Serialize(writer, checked((ulong)Container.Count));
                foreach (var kv in Container)
                {
                    Branch.Tree.SerializeLocator(writer, kv.Key);
                    kv.Key.OrderedSetPersist.Write(writer, kv.Value);
                }

                IsModified = false;
            }

            public override void Load(Stream stream)
            {
                var reader = new BinaryReader(stream);
                if (reader.ReadByte() != VERSION)
                    throw new Exception("Invalid LeafNode version.");

                var id = (long)CountCompression.Deserialize(reader);
                if (id != Branch.NodeHandle)
                    throw new Exception("Wtree logical error.");

                var count = (int)CountCompression.Deserialize(reader);
                for (var i = 0; i < count; i++)
                {
                    var path = Branch.Tree.DeserializeLocator(reader);
                    var data = path.OrderedSetPersist.Read(reader);
                    Container[path] = data;

                    RecordCount += data.Count;
                }

                IsModified = false;
            }

            public IOrderedSet<IData, IData> FindData(Locator locator, Direction direction, ref FullKey nearFullKey, ref bool hasNearFullKey)
            {
                IOrderedSet<IData, IData> data = null;
                Container.TryGetValue(locator, out data);
                if (direction == Direction.None)
                    return data;

                if (Container.Count == 1 && data != null)
                    return data;

                IOrderedSet<IData, IData> nearData = null;
                if (direction == Direction.Backward)
                {
                    var havePrev = false;
                    var prev = default(Locator);

                    foreach (var kv in Container)
                    {
                        if (kv.Key.CompareTo(locator) < 0)
                        {
                            if (!havePrev || kv.Key.CompareTo(prev) > 0)
                            {
                                prev = kv.Key;
                                nearData = kv.Value;
                                havePrev = true;
                            }
                        }
                    }

                    if (havePrev)
                    {
                        hasNearFullKey = true;
                        nearFullKey = new FullKey(prev, nearData.Last.Key);
                    }
                }
                else //if (direction == Direction.Forward)
                {
                    var haveNext = false;
                    var next = default(Locator);

                    foreach (var kv in Container)
                    {
                        if (kv.Key.CompareTo(locator) > 0)
                        {
                            if (!haveNext || kv.Key.CompareTo(next) < 0)
                            {
                                next = kv.Key;
                                nearData = kv.Value;
                                haveNext = true;
                            }
                        }
                    }

                    if (haveNext)
                    {
                        hasNearFullKey = true;
                        nearFullKey = new FullKey(next, nearData.First.Key);
                    }
                }

                return data;
            }
        }
    }
}
