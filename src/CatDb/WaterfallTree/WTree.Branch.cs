namespace CatDb.WaterfallTree;
public partial class WTree
{
    private partial class Branch
    {
        public readonly WTree Tree;
        public BranchCache Cache = new();

        /// <summary>
        /// on load
        /// </summary>
        public Branch(WTree tree, NodeType nodeType, long nodeHandle)
        {
            Tree = tree;
            NodeType = nodeType;
            NodeHandle = nodeHandle;
        }

        /// <summary>
        /// on brand new branch
        /// </summary>
        public Branch(WTree tree, NodeType nodeType)
            : this(tree, nodeType, tree._heap.ObtainNewHandle())
        {
            _node = Node.Create(this);
        }

        public override string ToString()
        {
            return
                $"NodeType = {NodeType}, Handle = {NodeHandle}, IsNodeLoaded = {IsNodeLoaded}, Cache.OperationCount = {Cache.OperationCount}";
        }

        #region Node

        public NodeType NodeType;

        /// <summary>
        /// permanent and unique node handle 
        /// </summary>
        public long NodeHandle { get; set; }
        
        public volatile NodeState NodeState;

        public bool IsNodeLoaded => _node != null;

        private Node? _node;

        public Node Node
        {
            get
            {
                if (_node != null)
                    return _node;

                _node = Tree.Retrieve(NodeHandle);

                if (_node != null)
                {
                    // Node is already registered in the cache — just take ownership.
                    // Do NOT call Packet: it would assert because the key is already present.
                    _node.Branch = this;
                }
                else
                {
                    // Load from disk; Node.Create registers it in the cache via Packet.
                    _node = Node.Create(this);
                    _node.Load();
                }

                return _node;
            }
            set => _node = value;
        }

        #endregion
    }
}
