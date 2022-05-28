using System.Diagnostics;
using CatDb.General.Compression;
using CatDb.General.Comparers;

namespace CatDb.WaterfallTree
{
    public partial class WTree
    {
        [DebuggerDisplay("Count = {Count}")]
        private class BranchCollection : List<KeyValuePair<FullKey, Branch>>
        {
            private static readonly KeyValuePairComparer<FullKey, Branch> Comparer = new(Comparer<FullKey>.Default);

            public BranchCollection()
            {
            }

            public BranchCollection(int capacity)
                : base(capacity)
            {
            }


            public BranchCollection(params KeyValuePair<FullKey, Branch>[] array)
                : base(array)
            {
            }

            public BranchCollection(IEnumerable<KeyValuePair<FullKey, Branch>> collection)
                : base(collection)
            {
            }

            public int BinarySearch(FullKey locator, int index, int length, IComparer<KeyValuePair<FullKey, Branch>> comparer)
            {
                return BinarySearch(index, length, new KeyValuePair<FullKey, Branch>(locator, null), comparer);
            }

            public int BinarySearch(FullKey locator, int index, int length)
            {
                return BinarySearch(locator, index, length, Comparer);
            }

            public int BinarySearch(FullKey locator)
            {
                return BinarySearch(locator, 0, Count);
            }

            public void Add(FullKey locator, Branch branch)
            {
                Add(new KeyValuePair<FullKey, Branch>(locator, branch));
            }

            public IEnumerable<KeyValuePair<FullKey, Branch>> Range(int fromIndex, int toIndex)
            {
                for (var i = fromIndex; i <= toIndex; i++)
                    yield return this[i];
            }

            public void Store(WTree tree, BinaryWriter writer)
            {
                CountCompression.Serialize(writer, checked((ulong)Count));

                Debug.Assert(Count > 0);
                writer.Write((byte)this[0].Value.NodeType);

                for (var i = 0; i < Count; i++)
                {
                    var kv = this[i];
                    var fullkey = kv.Key;
                    var branch = kv.Value;
                    //lock (branch)
                    //{
                    //}

                    //write locator
                    tree.SerializeLocator(writer, fullkey.Locator);
                    fullkey.Locator.KeyPersist.Write(writer, fullkey.Key);

                    //write branch info
                    writer.Write(branch.NodeHandle);                    
                    writer.Write((int)branch.NodeState);
                    
                    branch.Cache.Store(tree, writer);                    
                }
            }

            public void Load(WTree tree, BinaryReader reader)
            {
                var count = (int)CountCompression.Deserialize(reader);
                Capacity = count;

                var nodeType = (NodeType)reader.ReadByte();

                for (var i = 0; i < count; i++)
                {
                    //read fullKey
                    var locator = tree.DeserializeLocator(reader);
                    var key = locator.KeyPersist.Read(reader);
                    var fullKey = new FullKey(locator, key);

                    //read branch info
                    var nodeId = reader.ReadInt64();
                    var nodeState = (NodeState)reader.ReadInt32();

                    var branch = new Branch(tree, nodeType, nodeId)
                    {
                        NodeState = nodeState
                    };

                    branch.Cache.Load(tree, reader);

                    Add(new KeyValuePair<FullKey, Branch>(fullKey, branch));
                }
            }
        }
    }
}
