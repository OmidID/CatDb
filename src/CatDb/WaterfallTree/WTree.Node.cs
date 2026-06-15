// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using System.Diagnostics;

namespace CatDb.WaterfallTree;
public partial class WTree
{
    private abstract class Node
    {
        public bool IsModified { get; protected set; }
        public Branch Branch;
        public volatile bool IsExpiredFromCache;

        // ── Incremental-checkpoint LSN tracking (TransactionLog) ──────────────
        // MinDirtyLsn = oldest applied-but-unflushed op LSN (long.MaxValue when clean) — pins how far the
        // checkpoint LSN may advance (recovery replays strictly after min(MinDirtyLsn) over loaded dirty
        // nodes). PageLsn = max op LSN reflected in this node's image (persisted but currently advisory:
        // recovery uses idempotent in-order replay, NOT a redo-skip, because waterfall ops sink to leaves
        // out of LSN order so PageLsn≥op does NOT prove op is applied). All inert unless TransactionLog +
        // IncrementalCheckpoint.
        public long MinDirtyLsn = long.MaxValue;
        public long PageLsn;
        public long StructuralLsn;

        /// <summary>True until this node has been written to the heap at least once. A split creates a node
        /// with a fresh handle and no image yet; the incremental checkpoint must store every NeverStored node
        /// so no flushed parent ever references a non-existent child image (recovery would fail to load it).</summary>
        public bool NeverStored = true;

        /// <summary>Transient per-checkpoint flag: set by the incremental checkpoint's selection pass for the
        /// nodes it will flush this round (dirty internals + new nodes + the coldest dirty leaves); the
        /// Store-fall persists only flagged nodes and clears it. Always false outside an incremental flush.</summary>
        public bool ToCheckpoint;

        /// <summary>Folds an applied op-batch's LSNs into MinDirtyLsn (oldest unflushed) and PageLsn (max
        /// reflected). Called from Apply under the branch lock. No-op for unstamped (lsn 0) ops.</summary>
        protected void TrackAppliedLsn(IOperationCollection operations)
        {
            for (var i = 0; i < operations.Count; i++)
            {
                var l = operations[i].Lsn;
                if (l == 0) continue;
                if (l < MinDirtyLsn) MinDirtyLsn = l;
                if (l > PageLsn) PageLsn = l;
            }
        }

        /// <summary>Last known serialized size — used as the in-memory footprint estimate for the
        /// byte-budgeted cache. Internal (buffer-laden) nodes are far larger than leaves, so the cache
        /// must bound by bytes, not node count, to keep the managed heap (and GC pauses) flat.</summary>
        public long ApproxByteSize;
#if DEBUG
#pragma warning disable CS0649
        public volatile int TaskId;
#pragma warning restore CS0649
#endif
        private static long _globalTouchId;
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
#if PERFORMANCE_CHECK
            var perfStart = Stopwatch.GetTimestamp();
#endif

            using var stream = new MemoryStream();
            Store(stream);
            ApproxByteSize = stream.Length;

#if PERFORMANCE_CHECK
            CatDb.General.Diagnostics.PerformanceCheck.Observe("wtree.node.store.bytes", stream.Length);
            if (Type == NodeType.Internal)
                CatDb.General.Diagnostics.PerformanceCheck.Increment("wtree.node.store.internal");
            else
                CatDb.General.Diagnostics.PerformanceCheck.Increment("wtree.node.store.leaf");
#endif

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
            NeverStored = false;  // now has an on-disk image — safe for a parent to reference its handle

#if PERFORMANCE_CHECK
            CatDb.General.Diagnostics.PerformanceCheck.ObserveDurationTicks("wtree.node.store", perfStart);
#endif
        }

        public void Load()
        {
            var heap = Branch.Tree._heap;
            var buffer = heap.Read(Branch.NodeHandle);
            ApproxByteSize = buffer.Length;
            Load(new MemoryStream(buffer));
            NeverStored = false;  // loaded from an existing on-disk image
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
