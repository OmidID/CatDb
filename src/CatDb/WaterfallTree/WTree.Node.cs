using System.Diagnostics;

namespace CatDb.WaterfallTree
{
    public partial class WTree
    {
        private abstract class Node
        {
            public bool IsModified { get; protected set; }
            public Branch Branch;
            public volatile bool IsExpiredFromCache;
#if DEBUG
            public volatile int TaskId;
#endif
            private static long _globalTouchId = 0;
            private long _touchId;

            public long TouchId
            {
                get => Interlocked.Read(ref _touchId);
                set => Interlocked.Exchange(ref _touchId, value);
            }

            public Node(Branch branch)
            {
                Branch = branch;
            }

            public abstract void Apply(IOperationCollection operations);
            public abstract Node Split();
            public abstract void Merge(Node node);
            public abstract bool IsOverflow { get; }
            public abstract bool IsUnderflow { get; }
            public abstract FullKey FirstKey { get; }

            public abstract void Store(Stream stream);
            public abstract void Load(Stream stream);

            public void Touch(long count)
            {
                Debug.Assert(count > 0);
                _touchId = Interlocked.Add(ref _globalTouchId, count);

                //IsExpiredFromCache = false;
            }

            //only for speed reason
            public NodeType Type => Branch.NodeType;

            public bool IsRoot => ReferenceEquals(Branch.Tree._rootBranch, Branch);

            public NodeState State
            {
                get
                {
                    if (IsOverflow)
                        return NodeState.Overflow;

                    if (IsUnderflow)
                        return NodeState.Underflow;

                    return NodeState.None;
                }
            }

            public void Store()
            {
                using var stream = new MemoryStream();
                Store(stream);

                //int recordCount = 0;
                //string type = "";
                //if (this is InternalNode)
                //{
                //    recordCount = ((InternalNode)this).Branch.Cache.OperationCount;
                //    type = "Internal";
                //}
                //else
                //{
                //    recordCount = ((LeafNode)this).RecordCount;
                //    type = "Leaf";
                //}
                //double sizeInMB = Math.Round(stream.Length / (1024.0 * 1024), 2);
                //Console.WriteLine("{0} {1}, Records {2}, Size {3} MB", type, Branch.NodeHandle, recordCount, sizeInMB);

                Branch.Tree._heap.Write(Branch.NodeHandle, stream.GetBuffer(), 0, (int)stream.Length);
            }

            public void Load()
            {
                var heap = Branch.Tree._heap;
                var buffer = heap.Read(Branch.NodeHandle);
                Load(new MemoryStream(buffer));
            }

            public static Node Create(Branch branch)
            {
                Node node = branch.NodeType switch
                {
                    NodeType.Leaf => new LeafNode(branch, true),
                    NodeType.Internal => new InternalNode(branch, new BranchCollection(), true),
                    _ => throw new NotSupportedException()
                };

                branch.Tree.Packet(node.Branch.NodeHandle, node);
                return node;
            }
        }

        public enum NodeState
        {
            None,
            Overflow,
            Underflow
        }

        protected enum NodeType : byte
        {
            Leaf,
            Internal
        }
    }
}
