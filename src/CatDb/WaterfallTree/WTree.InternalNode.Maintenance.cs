// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
#if PERFORMANCE_CHECK
            var perfStart = Stopwatch.GetTimestamp();
#endif

            if (HaveChildrenForMaintenance)
            {
                var helpers = new MaintenanceHelper[Branches.Count];
                for (var index = Branches.Count - 1; index >= 0; index--)
                    helpers[index] = new MaintenanceHelper(level, token, helpers, Branches[index], index);

                Branches.Clear();

                try
                {
                    // Run right to left: when helper[i] checks its right neighbor helpers[i+1],
                    // that neighbor is already fully processed — no Task, no Wait needed.
                    for (var index = helpers.Length - 1; index >= 0; index--)
                        helpers[index].Run();
                }
                finally
                {
                    // ALWAYS rebuild Branches from helpers, even if Run() threw.
                    // Each helper's List is initialized before Run() processes it,
                    // so we can safely collect whatever was built.
                    for (var index = 0; index < helpers.Length; index++)
                    {
                        if (helpers[index].List != null)
                            Branches.AddRange(helpers[index].List);
                    }

                    RebuildOptimizator();
                    HaveChildrenForMaintenance = false;
                    IsModified = true;
                }
            }

            //sink branches — allocation-free: no LINQ, no closures
            var operationCount = 0;
            for (var i = 0; i < Branches.Count; i++)
                operationCount += Branches[i].Value.Cache.OperationCount;

            if (operationCount > Branch.Tree._internalNodeMaxOperations)
            {
#if PERFORMANCE_CHECK
                var initialOperationCount = operationCount;
                var totalSinkedOperations = 0;
                var coldBranchesSkipped = 0;
                var coldBranchesForced = 0;
#endif

                // Sort descending by OperationCount without LINQ.
                // Use a simple insertion into a local array (Branches.Count is small, typically ≤ MaxBranches).
                var branchCount = Branches.Count;
                Span<int> indices = branchCount <= 128
                    ? stackalloc int[branchCount]
                    : new int[branchCount];

                var sinkCount = 0;
                for (var i = 0; i < branchCount; i++)
                {
                    if (Branches[i].Value.Cache.OperationCount > 0)
                        indices[sinkCount++] = i;
                }

                // Sort the indices by descending OperationCount (insertion sort — N is small)
                for (var i = 1; i < sinkCount; i++)
                {
                    var key = indices[i];
                    var keyVal = Branches[key].Value.Cache.OperationCount;
                    var j = i - 1;
                    while (j >= 0 && Branches[indices[j]].Value.Cache.OperationCount < keyVal)
                    {
                        indices[j + 1] = indices[j];
                        j--;
                    }
                    indices[j + 1] = key;
                }

                for (var i = 0; i < sinkCount; i++)
                {
                    var branch = Branches[indices[i]].Value;

                    if (!branch.IsNodeLoaded)
                    {
                        // Cold-branch bounding: only force-sink unloaded branches when
                        // accumulation has grown far beyond normal operating range.
                        // At normal scale, hot-branch sinking keeps total near max.
                        // Cold accumulation only becomes problematic at large scale when
                        // many cold branches each hold moderate counts that sum unboundedly.
                        //
                        // Threshold: 3× max prevents unnecessary disk I/O at small scale
                        // while still bounding accumulation. Per-branch min (max/4) avoids
                        // loading a node from disk for trivial savings.
                        if (operationCount <= 3 * Branch.Tree._internalNodeMaxOperations)
                        {
#if PERFORMANCE_CHECK
                            coldBranchesSkipped++;
#endif
                            continue;
                        }
                        if (branch.Cache.OperationCount < Branch.Tree._internalNodeMaxOperations / 4)
                        {
#if PERFORMANCE_CHECK
                            coldBranchesSkipped++;
#endif
                            continue;
                        }

#if PERFORMANCE_CHECK
                        coldBranchesForced++;
#endif
                    }

                    operationCount -= branch.Cache.OperationCount;
#if PERFORMANCE_CHECK
                    totalSinkedOperations += branch.Cache.OperationCount;
#endif
                    if (branch.Fall(level, token, new Params(WalkMethod.Current, WalkAction.None, null, true)))
                        IsModified = true;

                    if (operationCount <= Branch.Tree._internalNodeMinOperations)
                        break;
                }

#if PERFORMANCE_CHECK
                CatDb.General.Diagnostics.PerformanceCheck.Observe("wtree.maintenance.sink.initial.ops", initialOperationCount);
                CatDb.General.Diagnostics.PerformanceCheck.Observe("wtree.maintenance.sink.final.ops", operationCount);
                CatDb.General.Diagnostics.PerformanceCheck.Observe("wtree.maintenance.sink.applied.ops", totalSinkedOperations);
                CatDb.General.Diagnostics.PerformanceCheck.Observe("wtree.maintenance.sink.cold.skipped", coldBranchesSkipped);
                CatDb.General.Diagnostics.PerformanceCheck.Observe("wtree.maintenance.sink.cold.forced", coldBranchesForced);
                CatDb.General.Diagnostics.PerformanceCheck.Observe("wtree.maintenance.sink.branch.count", branchCount);
#endif
            }

#if PERFORMANCE_CHECK
            CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("wtree.maintenance", perfStart);
#endif
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
                branch.SyncRoot.Enter();
                try
                {
                    var node = branch.Node;
                    rightNode = node.Split();
                    branch.NodeState = node.State;
                }
                finally
                {
                    branch.SyncRoot.Exit();
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

                branch.SyncRoot.Enter();
                try { branch.Node.Merge(node); }
                finally { branch.SyncRoot.Exit(); }
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
