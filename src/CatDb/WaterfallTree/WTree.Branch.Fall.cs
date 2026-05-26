#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Diagnostics;
using CatDb.Data;

namespace CatDb.WaterfallTree;
public partial class WTree
{
    private partial class Branch
    {
        // DoFall executes synchronously — no task, no state boxing, no semaphore.
        private void DoFall(BranchCache? cache, int level, Token? token, Params param)
        {
            var node = Node;
            // After parent eviction/reload, a sibling branch may have reclaimed this node
            // from cache (setting node.Branch to itself). Reassign ownership defensively.
            node.Branch = this;

            node.Touch(level);

            //1. Apply cache
            if (cache != null)
            {
                if (cache.Count == 1 || node.Type == NodeType.Leaf)
                {
                    foreach (var kv in cache)
                    {
                        //compact operations
                        kv.Key.Apply.Internal(kv.Value);

                        //apply
                        if (kv.Value.Count > 0)
                            node.Apply(kv.Value);
                    }
                }
                else
                {
                    // Sequential: we are already inside lock(this) which is held under
                    // lock(_rootBranch). Parallel.ForEach here would grab ThreadPool threads
                    // that are all blocked on lock(_rootBranch) from concurrent callers —
                    // causing ThreadPool starvation that gets progressively worse over time.
                    foreach (var kv in cache)
                    {
                        //compact operations
                        kv.Key.Apply.Internal(kv.Value);

                        //apply
                        if (kv.Value.Count > 0)
                            node.Apply(kv.Value);
                    }
                }

                cache.Clear();
            }

            //2. Maintenance
            // Skip during CacheFlush walks — CacheFlush is for evicting cold nodes, not for sinking
            // accumulated operations. Running Maintenance here causes cascading Sink Falls (disk I/O)
            // under the root lock, which is the primary cause of the performance cliff.
            // Skip during NoMaintenance — this is a child being sinked by a parent's Maintenance;
            // allowing recursive Maintenance would cause O(depth * B) cascading I/Os.
            if (node.Type == NodeType.Internal
                && (param.WalkAction & WalkAction.CacheFlush) == 0
                && (param.WalkAction & WalkAction.NoMaintenance) == 0)
                ((InternalNode)node).Maintenance(level, token);
            NodeState = node.State;

            if (node.IsExpiredFromCache && (param.WalkAction & WalkAction.CacheFlush) == WalkAction.CacheFlush)
            {
                // Upgrade to Store|Unload for this node only.
                // Use WalkMethod.Current so BroadcastFall is NOT called — children stay in cache
                // with their current state. The InternalNode.Store() below serialises child branch
                // caches, preserving all pending operations on disk.
                param = new Params(WalkMethod.Current, WalkAction.Store | WalkAction.Unload, param.WalkParams, param.Sink);
            }

            if (node.Type == NodeType.Internal)
            {
                if (param.WalkMethod != WalkMethod.Current)
                {
                    //broadcast
                    ((InternalNode)node).BroadcastFall(level, token, param);
                }
            }

            if ((param.WalkAction & WalkAction.Store) == WalkAction.Store)
            {
                if (node.IsModified)
                    node.Store();
            }

            if ((param.WalkAction & WalkAction.Unload) == WalkAction.Unload)
            {
                Node = null;
                Tree.Exclude(NodeHandle);
            }
        }

        public bool Fall(int level, Token token, Params param, TaskCreationOptions taskCreationOptions = TaskCreationOptions.None)
        {
            lock (this)
            {
                if (token != null && token.Cancellation.IsCancellationRequested)
                    return false;

                var haveSink = false;
                BranchCache? cache = null;
                if (param.Sink)
                {
                    if (Cache.OperationCount > 0)
                    {
                        if (param.IsTotal)
                        {
                            cache = Cache;
                            Cache = new BranchCache();
                            haveSink = true;
                        }
                        else //no matter IsOverall or IsPoint, we exclude all the operations for the path
                        {
                            var operationCollection = Cache.Exclude(param.Path);
                            if (operationCollection != null)
                            {
                                cache = new BranchCache(/*param.Path,*/ operationCollection);
                                haveSink = true;
                            }
                        }
                    }
                }

                DoFall(cache, level - 1, token, param);

                return haveSink;
            }
        }

        /// <summary>
        /// Falls are now synchronous; this is a no-op kept for call-site compatibility.
        /// </summary>
        public void WaitFall()
        {
        }

        public void ApplyToCache(Locator locator, IOperation operation)
        {
            lock (this)
                Cache.Apply(locator, operation);
        }

        public void ApplyToCache(IOperationCollection operations)
        {
            lock (this)
                Cache.Apply(operations);
        }

        public void ApplyToCache(IOperationCollection operations, int startIndex, int count)
        {
            lock (this)
                Cache.Apply(operations, startIndex, count);
        }

        public void MaintenanceRoot(Token token)
        {
            if (_node.IsOverflow)
            {
                var newBranch = new Branch(Tree, NodeType, NodeHandle)
                {
                    Node = Node
                };
                newBranch.Node.Branch = newBranch;
                newBranch.NodeState = newBranch.Node.State;

                NodeType = NodeType.Internal;
                //NodeHandle = Tree.Repository.Reserve();
                NodeHandle = Tree._heap.ObtainNewHandle();
                Node = Node.Create(this);
                NodeState = NodeState.None;

                Tree._depth++;

                var rootNode = (InternalNode)Node;
                rootNode.Branches.Add(new FullKey(Tree.MinLocator, null), newBranch);
                //rootNode.Branches.Add(newBranch.Node.FirstKey, newBranch);
                rootNode.HaveChildrenForMaintenance = true;
                rootNode.Maintenance(Tree._depth + 1, token);
            }
            else if (_node.IsUnderflow)
            {
                //TODO: also to release handle
                //Debug.Assert(node.Type == NodeType.Internal);

                //Branch branch = ((InternalNode)Node).Branches[0].Value;

                //NodeType = branch.NodeType;
                //NodeHandle = branch.NodeHandle;
                //Node = branch.node;
                //NodeState = branch.NodeState;

                //Tree.Depth--;
            }
        }
    }

    private enum WalkMethod
    {
        Current,
        CascadeFirst,
        CascadeLast,
        Cascade,
        CascadeButOnlyLoaded,
    }

    [Flags]
    private enum WalkAction
    {
        None = 0,
        Store = 0x01,
        Unload = 0x02,
        CacheFlush = 0x04,
        /// <summary>
        /// Skip Maintenance during DoFall — prevents recursive cascading.
        /// Used by Maintenance when sinking child branches so that a single Sink
        /// pushes operations only ONE level at a time (B-epsilon tree amortization).
        /// </summary>
        NoMaintenance = 0x08,
    }

    private class WalkParams
    {
    }

    private class CacheWalkParams : WalkParams
    {
        public long TouchId;

        public CacheWalkParams(long touchId)
        {
            TouchId = touchId;
        }
    }

    private class Params
    {
        public readonly WalkMethod WalkMethod;
        public readonly WalkAction WalkAction;
        public readonly WalkParams WalkParams;

        #region param scope

        public readonly Locator? Path;
        public readonly IData? FromKey;
        public readonly IData? ToKey;
        public readonly bool IsPoint;
        public readonly bool IsOverall;
        public readonly bool IsTotal;

        #endregion

        public readonly bool Sink;

        public Params(WalkMethod walkMethod, WalkAction walkAction, WalkParams walkParams, bool sink, Locator path, IData fromKey, IData toKey)
        {
            WalkMethod = walkMethod;
            WalkAction = walkAction;
            WalkParams = walkParams;

            Sink = sink;

            Path = path;
            FromKey = fromKey;
            ToKey = toKey;
            IsPoint = false;
            IsOverall = false;
            IsTotal = false;
        }

        public Params(WalkMethod walkMethod, WalkAction walkAction, WalkParams walkParams, bool sink, Locator path, IData key)
        {
            WalkMethod = walkMethod;
            WalkAction = walkAction;
            WalkParams = walkParams;

            Sink = sink;

            Path = path;
            FromKey = key;
            ToKey = key;
            IsPoint = true;
            IsOverall = false;
            IsTotal = false;
        }

        public Params(WalkMethod walkMethod, WalkAction walkAction, WalkParams walkParams, bool sink, Locator path)
        {
            WalkMethod = walkMethod;
            WalkAction = walkAction;
            WalkParams = walkParams;

            Sink = sink;

            Path = path;
            IsPoint = false;
            IsOverall = true;
            IsTotal = false;
        }

        public Params(WalkMethod walkMethod, WalkAction walkAction, WalkParams walkParams, bool sink)
        {
            WalkMethod = walkMethod;
            WalkAction = walkAction;
            WalkParams = walkParams;

            Sink = sink;

            IsPoint = false;
            IsOverall = false;
            IsTotal = true;
        }
    }

    /// <summary>
    /// Passed through the tree walk to carry cancellation.
    /// CountdownEvent and SemaphoreSlim have been removed — falls are fully synchronous.
    /// </summary>
    private class Token
    {
        public readonly CancellationToken Cancellation;

        [DebuggerStepThrough]
        public Token(CancellationToken cancellationToken)
        {
            Cancellation = cancellationToken;
        }
    }
}
