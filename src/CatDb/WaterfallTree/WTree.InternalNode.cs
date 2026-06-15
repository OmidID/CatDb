// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Diagnostics;
using CatDb.Data;
using CatDb.General.Compression;
using CatDb.General.Extensions;

namespace CatDb.WaterfallTree;
public partial class WTree
{
    private sealed partial class InternalNode : Node
    {
        private const byte VERSION = 41;     // v41 adds persisted PageLsn after the version byte
        private const byte VERSION_V40 = 40;  // pre-PageLsn images (PageLsn defaults to 0 → full replay)

        private readonly BranchesOptimizator _optimizator = new();

        public readonly BranchCollection Branches;

        public InternalNode(Branch branch)
            : this(branch, new BranchCollection(), false)
        {
        }

        public InternalNode(Branch branch, BranchCollection branches, bool isModified)
            : base(branch)
        {
            Debug.Assert(branch.NodeType == NodeType.Internal);

            Branches = branches;
            IsModified = isModified;
        }

        private void SequentialApply(IOperationCollection operations)
        {
            var locator = operations.Locator;

            if (Branches.Count == 0)
                return;

            var last = Branches[Branches.Count - 1];
            if (ReferenceEquals(last.Key.Locator, locator) && locator.KeyComparer.Compare(last.Key.Key, operations[0].FromKey) <= 0)
            {
                var branch = last.Value;
                branch.ApplyToCache(operations);
                if (branch.NodeState != NodeState.None)
                    HaveChildrenForMaintenance = true;

                return;
            }

            var range = _optimizator.FindRange(locator);

            if (!range.IsBaseLocator)
            {
                var branch = Branches[range.LastIndex].Value;
                branch.ApplyToCache(operations);
                if (branch.NodeState != NodeState.None)
                    HaveChildrenForMaintenance = true;

                return;
            }

            var index = 0;

            for (var i = range.FirstIndex; i <= range.LastIndex; i++)
            {
                var key = Branches[i].Key.Key;

                var idx = operations.BinarySearch(key, index, operations.Count - index);
                if (idx < 0)
                    idx = ~idx;
                idx--;

                var count = idx - index + 1;
                if (count > 0)
                {
                    var branchIndex = Math.Max(0, i - 1);
                    var branch = Branches[branchIndex].Value;

                    if (count < operations.Count)
                        branch.ApplyToCache(operations, index, count);
                    else
                        branch.ApplyToCache(operations);
                    if (branch.NodeState != NodeState.None)
                        HaveChildrenForMaintenance = true;

                    index += count;
                }
            }

            if (operations.Count - index > 0)
            {
                var branch = Branches[range.LastIndex].Value;
                var remainCount = operations.Count - index;

                Debug.Assert(Branches[range.LastIndex].Key.Locator.Equals(operations.Locator));
                Debug.Assert(operations.Locator.KeyComparer.Compare(Branches[range.LastIndex].Key.Key, operations[index].FromKey) <= 0);

                if (index > 0)
                    branch.ApplyToCache(operations, index, remainCount);
                else
                    branch.ApplyToCache(operations);
                if (branch.NodeState != NodeState.None)
                    HaveChildrenForMaintenance = true;
            }
        }

        public override void Apply(IOperationCollection operations)
        {
            Debug.Assert(operations.Count > 0);

            if (operations.AreAllMonotoneAndPoint)
            {
                SequentialApply(operations); //sequential mode optimization
                IsModified = true;
                TrackAppliedLsn(operations);
                return;
            }

            var locator = operations.Locator;
            var range = _optimizator.FindRange(locator);

            foreach (var operation in operations)
            {
                int firstIndex, lastIndex;

                switch (operation.Scope)
                {
                    case OperationScope.Point:
                        {
                            firstIndex = lastIndex = _optimizator.FindIndex(range, locator, operation.FromKey);
                            Debug.Assert(firstIndex >= 0);
                        }
                        break;
                    case OperationScope.Range:
                        {
                            firstIndex = _optimizator.FindIndex(range, locator, operation.FromKey);
                            if (firstIndex < 0)
                                firstIndex = 0;
                            lastIndex = _optimizator.FindIndex(range, locator, operation.ToKey);
                        }
                        break;
                    case OperationScope.Overall:
                        {
                            firstIndex = range.FirstIndex;
                            if (range.IsBaseLocator && range.FirstIndex > 0)
                                firstIndex--;

                            lastIndex = range.LastIndex;
                        }
                        break;
                    default:
                        throw new NotSupportedException(operation.Scope.ToString());
                }

                for (var i = firstIndex; i <= lastIndex; i++)
                {
                    var branch = Branches[i].Value;

                    branch.ApplyToCache(locator, operation);

                    if (branch.NodeState != NodeState.None)
                        HaveChildrenForMaintenance = true;
                }
            }

            IsModified = true;
            TrackAppliedLsn(operations);
        }

        public override Node Split()
        {
            var rightBranch = new Branch(Branch.Tree, NodeType.Internal);
            var rightNode = (InternalNode)rightBranch.Node;

            var leftCount = 0;
            var leftBranchesCount = Branches.Count / 2;

            for (var i = 0; i < leftBranchesCount; i++)
                leftCount += Branches[i].Value.Cache.OperationCount;

            rightNode.Branches.AddRange(Branches, leftBranchesCount, Branches.Count - leftBranchesCount);
            Branches.RemoveRange(leftBranchesCount, Branches.Count - leftBranchesCount);

            RebuildOptimizator();
            rightNode.RebuildOptimizator();

            rightNode.TouchId = TouchId;

            IsModified = true;

            return rightNode;
        }

        public override void Merge(Node node)
        {
            var rightNode = (InternalNode)node;
            Branches.AddRange(rightNode.Branches);

            RebuildOptimizator();
            rightNode.RebuildOptimizator();

            if (TouchId < node.TouchId)
                TouchId = node.TouchId;

            IsModified = true;
        }

        public void BroadcastFall(int level, Token token, Params param)
        {
            int firstIndex, lastIndex;
            if (param.IsTotal)
            {
                firstIndex = 0;
                lastIndex = Branches.Count - 1;
            }
            else
            {
                var range = _optimizator.FindRange(param.Path);
                if (param.IsPoint)
                {
                    firstIndex = lastIndex = _optimizator.FindIndex(range, param.Path, param.FromKey);
                    Debug.Assert(firstIndex >= 0);
                }
                else if (param.IsOverall)
                {
                    firstIndex = range.FirstIndex;
                    if (range.IsBaseLocator && range.FirstIndex > 0)
                        firstIndex--;

                    lastIndex = range.LastIndex;
                }
                else
                {
                    firstIndex = _optimizator.FindIndex(range, param.Path, param.FromKey);
                    if (firstIndex < 0)
                        firstIndex = 0;
                    lastIndex = _optimizator.FindIndex(range, param.Path, param.ToKey);
                }
            }

            // Defensive: clamp indices to valid bounds
            var branchCount = Branches.Count;
            if (branchCount == 0)
                return;
            if (firstIndex >= branchCount)
                firstIndex = branchCount - 1;
            if (lastIndex >= branchCount)
                lastIndex = branchCount - 1;
            if (firstIndex < 0)
                firstIndex = 0;

            // Resolve actual iteration range based on walk method (no allocation)
            int iterFirst, iterLast;
            switch (param.WalkMethod)
            {
                case WalkMethod.CascadeFirst:
                    iterFirst = iterLast = firstIndex;
                    break;
                case WalkMethod.CascadeLast:
                    iterFirst = iterLast = lastIndex;
                    break;
                case WalkMethod.Cascade:
                case WalkMethod.CascadeButOnlyLoaded:
                    iterFirst = firstIndex;
                    iterLast = lastIndex;
                    break;
                default:
                    throw new NotSupportedException(param.WalkMethod.ToString());
            }

            // Clamp to valid bounds after resolution
            if (iterLast >= branchCount)
                iterLast = branchCount - 1;

            // Falls are synchronous — direct indexed loop, zero allocation.
            for (var i = iterFirst; i <= iterLast; i++)
            {
                var branch = Branches[i].Value;

                if (param.WalkMethod == WalkMethod.CascadeButOnlyLoaded)
                {
                    if (!branch.IsNodeLoaded)
                        continue;
                }

                if (branch.Fall(level, token, param))
                    IsModified = true;
            }
        }

        public override bool IsOverflow => Branches.Count > Branch.Tree._internalNodeMaxBranches;

        public override bool IsUnderflow
        {
            get
            {
                if (IsRoot)
                    return Branches.Count < 2;

                return Branches.Count < Branch.Tree._internalNodeMinBranches;
            }
        }

        public override FullKey FirstKey => Branches[0].Key;

        public override void Store(Stream stream)
        {
            var writer = new BinaryWriter(stream);
            writer.Write(VERSION);
            writer.Write(PageLsn); // v41: max op LSN reflected (incremental-checkpoint redo-skip)

            CountCompression.Serialize(writer, checked((ulong)Branch.NodeHandle));

            writer.Write(HaveChildrenForMaintenance);
            Branches.Store(Branch.Tree, writer);

            IsModified = false;
            MinDirtyLsn = long.MaxValue;
        }

        public override void Load(Stream stream)
        {
            var reader = new BinaryReader(stream);
            var version = reader.ReadByte();
            if (version != VERSION && version != VERSION_V40)
                throw new Exception("Invalid InternalNode version.");

            PageLsn = version >= VERSION ? reader.ReadInt64() : 0;

            var id = (long)CountCompression.Deserialize(reader);
            if (id != Branch.NodeHandle)
                throw new Exception("Wtree logical error.");

            HaveChildrenForMaintenance = reader.ReadBoolean();
            Branches.Load(Branch.Tree, reader);

            RebuildOptimizator();

            IsModified = false;
        }

        private KeyValuePair<FullKey, Branch> FindFirstBranch(Range range, ref FullKey nearFullKey, ref bool hasNearFullKey)
        {
            int idx;
            if (!range.IsBaseLocator)
                idx = range.LastIndex;
            else
            {
                idx = range.FirstIndex;
                if (idx > 0)
                    idx--;
            }

            if (idx + 1 < Branches.Count)
            {
                hasNearFullKey = true;
                nearFullKey = Branches[idx + 1].Key;
            }

            return Branches[idx]; 
        }

        private KeyValuePair<FullKey, Branch> FindLastBranch(Range range, ref FullKey nearFullKey, ref bool hasNearFullKey)
        {
            var idx = range.LastIndex; //no matter of range.IsBaseLocator

            if (idx > 0)
            {
                hasNearFullKey = true;
                nearFullKey = Branches[idx - 1].Key;
            }

            return Branches[idx]; 
        }

        public Branch FindLastBranch(Locator locator, ref FullKey nearFullKey, ref bool hasNearFullKey)
        {
            var range = _optimizator.FindRange(locator);
            var idx = range.LastIndex; //no matter of range.IsBaseLocator

            if (idx > 0)
            {
                hasNearFullKey = true;
                nearFullKey = Branches[idx - 1].Key;
            }

            return Branches[idx].Value;
        }

        /// <summary>
        /// The hook.
        /// </summary>
        public KeyValuePair<FullKey, Branch> FindBranch(Locator locator, IData key, Direction direction, ref FullKey nearFullKey, ref bool hasNearFullKey)
        {
            var range = _optimizator.FindRange(locator);
            
            if (key == null)
            {
                return direction switch
                {
                    Direction.Forward => FindFirstBranch(range, ref nearFullKey, ref hasNearFullKey),
                    Direction.Backward => FindLastBranch(range, ref nearFullKey, ref hasNearFullKey),
                    _ => throw new NotSupportedException(direction.ToString())
                };
            }

            var idx = _optimizator.FindIndex(range, locator, key);
            Debug.Assert(idx >= 0);

            switch (direction)
            {
                case Direction.Backward:
                    {
                        if (idx > 0)
                        {
                            nearFullKey = Branches[idx - 1].Key;
                            hasNearFullKey = true;
                        }
                    }
                    break;
                case Direction.Forward:
                    {
                        if (idx < Branches.Count - 1)
                        {
                            hasNearFullKey = true;
                            nearFullKey = Branches[idx + 1].Key;
                        }
                    }
                    break;
            }

            return Branches[idx];
        }

        public void RebuildOptimizator()
        {
            _optimizator.Rebuild(Branches);
        }
    }
}
