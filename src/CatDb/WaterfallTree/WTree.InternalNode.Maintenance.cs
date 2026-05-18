#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Diagnostics;

namespace CatDb.WaterfallTree;
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

                // Run right to left: when helper[i] checks its right neighbor helpers[i+1],
                // that neighbor is already fully processed — no Task, no Wait needed.
                for (var index = helpers.Length - 1; index >= 0; index--)
                    helpers[index].Run();

                for (var index = 0; index < helpers.Length; index++)
                    Branches.AddRange(helpers[index].List);

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
                }
            }
        }

        private class MaintenanceHelper
        {
            private readonly int _level;
            private readonly Token _token;
            private readonly MaintenanceHelper[] _helpers;
            private readonly int _index;
            private readonly KeyValuePair<FullKey, Branch> _kv;

            public BranchCollection List;

            public MaintenanceHelper(int level, Token token, MaintenanceHelper[] helpers, KeyValuePair<FullKey, Branch> kv, int index)
            {
                _level = level;
                _token = token;
                _helpers = helpers;
                _index = index;
                _kv = kv;
            }

            private void Split(int index)
            {
                var branch = List[index].Value;

                Node rightNode;
                lock (branch)
                {
                    var node = branch.Node;
                    rightNode = node.Split();
                    branch.NodeState = node.State;
                }

                var rightBranch = rightNode.Branch;
                rightBranch.NodeState = rightNode.State;

                List.Insert(index + 1, new KeyValuePair<FullKey, Branch>(rightNode.FirstKey, rightBranch));
                if (rightNode.IsOverflow)
                    Split(index + 1);
                if (branch.NodeState == NodeState.Overflow)
                    Split(index);
            }

            private void Merge(Node node)
            {
                var branch = List[List.Count - 1].Value;
                Debug.Assert(branch.Cache.OperationCount == 0);

                lock (branch)
                    branch.Node.Merge(node);
                branch.NodeState = branch.Node.State;

                //release node space
                node.Branch.Tree._heap.Release(node.Branch.NodeHandle);
            }

            /// <summary>
            /// Synchronous equivalent of the former Task-based Do(object state).
            /// Must be called right-to-left so helpers[_index+1] is already Run() when
            /// this helper needs to inspect or merge with it.
            /// </summary>
            public void Run()
            {
                var branch = _kv.Value;
                var isFall = false;

                if (branch.NodeState == NodeState.None)
                    List = new BranchCollection(_kv);
                else
                {
                    branch.Fall(_level, _token, new Params(WalkMethod.Current, WalkAction.None, null, true));
                    isFall = true;

                    if (branch.NodeState == NodeState.None)
                        List = new BranchCollection(_kv);
                    else
                    {
                        List = new BranchCollection { _kv };

                        if (branch.NodeState == NodeState.Overflow)
                            Split(0);
                    }
                }

                if (_index + 1 >= _helpers.Length)
                    return;

                // Right neighbor is already processed — no Wait() needed.
                var h = _helpers[_index + 1];

                if (branch.NodeState == NodeState.Underflow || h.List[0].Value.NodeState == NodeState.Underflow)
                {
                    if (!isFall)
                        branch.Fall(_level, _token, new Params(WalkMethod.Current, WalkAction.None, null, true));

                    if (h.List[0].Value.Cache.OperationCount > 0)
                        h.List[0].Value.Fall(_level, _token, new Params(WalkMethod.Current, WalkAction.None, null, true));

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
