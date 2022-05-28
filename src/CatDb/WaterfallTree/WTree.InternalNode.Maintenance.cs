using System.Diagnostics;

namespace CatDb.WaterfallTree
{
    public partial class WTree
    {
        private sealed partial class InternalNode : Node
        {
            public volatile bool HaveChildrenForMaintenance;

            public void Maintenance(int level, Token token)
            {
                if (HaveChildrenForMaintenance)
                {
                    var helpers = new MaintenanceHelper[Branches.Count];
                    for (var index = Branches.Count - 1; index >= 0; index--)
                        helpers[index] = new MaintenanceHelper(level, token, helpers, Branches[index], index);

                    Branches.Clear();
                    for (var index = 0; index < helpers.Length; index++)
                    {
                        var helper = helpers[index];
                        helper.Task.Wait();
                        Branches.AddRange(helper.List);
                    }

                    RebuildOptimizator();

                    HaveChildrenForMaintenance = false;

                    IsModified = true;
                }

                //sink branches
                var operationCount = Branches.Sum(x => x.Value.Cache.OperationCount);
                if (operationCount > Branch.Tree._internalNodeMaxOperations)
                {
                    //Debug.WriteLine(string.Format("{0}: {1} = {2}", level, Branch.NodeHandle, operationCount));
                    foreach (var kv in Branches.Where(x => x.Value.Cache.OperationCount > 0).OrderByDescending(x => x.Value.Cache.OperationCount))
                    {
                        var branch = kv.Value;

                        operationCount -= branch.Cache.OperationCount;
                        if (branch.Fall(level, token, new Params(WalkMethod.Current, WalkAction.None, null, true)))
                            IsModified = true;

                        if (operationCount <= Branch.Tree._internalNodeMinOperations)
                            break;

                        //branch.WaitFall();
                    }
                }
            }

            private class MaintenanceHelper
            {
                private readonly int _level;
                private readonly Token _token;
                private readonly MaintenanceHelper[] _helpers;
                private readonly int _index;

                public BranchCollection List;
                public readonly Task Task;

                public MaintenanceHelper(int level, Token token, MaintenanceHelper[] helpers, KeyValuePair<FullKey, Branch> kv, int index)
                {
                    _level = level;
                    _token = token;
                    _helpers = helpers;
                    _index = index;
                    Task = Task.Factory.StartNew(Do, kv, TaskCreationOptions.AttachedToParent);
                }

                private void Split(int index)
                {
                    var node = List[index].Value.Node;
                    var branch = node.Branch;

                    var rightNode = node.Split();
                    node.Branch.NodeState = node.State;
                    var rightBranch = rightNode.Branch;

                    branch.NodeState = node.State;
                    rightBranch.NodeState = rightNode.State;

                    List.Insert(index + 1, new KeyValuePair<FullKey, Branch>(rightNode.FirstKey, rightBranch));
                    if (rightNode.IsOverflow)
                        Split(index + 1);
                    if (node.IsOverflow)
                        Split(index);
                }

                private void Merge(Node node)
                {
                    var branch = List[List.Count - 1].Value;
                    Debug.Assert(branch.Cache.OperationCount == 0);

                    branch.Node.Merge(node);
                    branch.NodeState = branch.Node.State;

                    //release node space
                    node.Branch.Tree._heap.Release(node.Branch.NodeHandle);
                }

                private void Do(object state)
                {
                    var kv = (KeyValuePair<FullKey, Branch>)state;
                    var branch = kv.Value;

                    var isFall = false;

                    branch.WaitFall();
                    if (branch.NodeState == NodeState.None)
                        List = new BranchCollection(kv);
                    else
                    {
                        branch.Fall(_level, _token, new Params(WalkMethod.Current, WalkAction.None, null, true));
                        branch.WaitFall();
                        isFall = true;

                        if (branch.NodeState == NodeState.None)
                            List = new BranchCollection(kv);
                        else
                        {
                            List = new BranchCollection { kv };

                            if (branch.NodeState == NodeState.Overflow)
                                Split(0);
                        }
                    }

                    if (_index + 1 >= _helpers.Length)
                        return;

                    var h = _helpers[_index + 1];
                    h.Task.Wait();

                    if (branch.NodeState == NodeState.Underflow || h.List[0].Value.NodeState == NodeState.Underflow)
                    {
                        if (!isFall)
                        {
                            branch.Fall(_level, _token, new Params(WalkMethod.Current, WalkAction.None, null, true));
                            branch.WaitFall();
                        }

                        if (h.List[0].Value.Cache.OperationCount > 0)
                        {
                            h.List[0].Value.Fall(_level, _token, new Params(WalkMethod.Current, WalkAction.None, null, true));
                            h.List[0].Value.WaitFall();
                        }

                        Debug.Assert(h.List[0].Value.Cache.OperationCount == 0);
                        Merge(h.List[0].Value.Node);
                        h.List.RemoveAt(0);
                    }

                    if (List[List.Count - 1].Value.NodeState == NodeState.Overflow)
                        Split(List.Count - 1);
                }
            }
        }
    }
}
