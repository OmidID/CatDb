// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Diagnostics;
using CatDb.Data;
using CatDb.General.Collections;
using CatDb.General.Compression;

namespace CatDb.WaterfallTree;
public partial class WTree
{
    private sealed class LeafNode : Node
    {
        private const byte VERSION = FormatVersion.Current;

        /// <summary>
        /// Total number of records in the node
        /// </summary>
        public int RecordCount { get; private set; }

        private readonly Dictionary<Locator, IOrderedSet<IData, IData>> _container;

        public LeafNode(Branch branch, bool isModified)
            : base(branch)
        {
            Debug.Assert(branch.NodeType == NodeType.Leaf);

            _container = new Dictionary<Locator, IOrderedSet<IData, IData>>();
            IsModified = isModified;
        }

        public override void Apply(IOperationCollection operations)
        {
            var locator = operations.Locator;

            if (_container.TryGetValue(locator, out var data))
            {
                var emptied = false;
                data.Lock.EnterWrite();
                try
                {
                    RecordCount -= data.Count;

                    if (locator.Apply.Leaf(operations, data))
                        IsModified = true;

                    RecordCount += data.Count;

                    if (data.Count == 0)
                    {
                        _container.Remove(locator);
                        emptied = true;
                    }
                }
                finally { data.Lock.ExitWrite(); }

                if (emptied)
                    NativeReclaim.Defer(data);   // dropped set — deferred native reclaim (grace period)
            }
            else
            {
                data = locator.OrderedSetFactory.Create();
                Debug.Assert(data != null);
                if (locator.Apply.Leaf(operations, data))
                    IsModified = true;

                RecordCount += data.Count;

                if (data.Count > 0)
                    _container.Add(locator, data);
            }

            TrackAppliedLsn(operations);
        }

        public override Node Split()
        {
            var halfRecordCount = RecordCount / 2;

            var rightBranch = new Branch(Branch.Tree, NodeType.Leaf);
            var rightNode = ((LeafNode)rightBranch.Node);
            var rightContainer = rightNode._container;

            var leftRecordCount = 0;

            var specialCase = new KeyValuePair<Locator, IOrderedSet<IData, IData>>(default(Locator), null);

            if (_container.Count == 1)
            {
                var kv = _container.First();
                IOrderedSet<IData, IData> data;
                kv.Value.Lock.EnterWrite();
                try { data = kv.Value.Split(halfRecordCount); }
                finally { kv.Value.Lock.ExitWrite(); }

                Debug.Assert(data.Count > 0);
                rightContainer.Add(kv.Key, data);
                leftRecordCount = RecordCount - data.Count;
            }
            else //if (Container.Count > 1)
            {
                using var enumerator = _container.OrderBy(x => x.Key).GetEnumerator();

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
                    if (leftRecordCount < halfRecordCount)
                        continue;

                    if (leftRecordCount > halfRecordCount)
                    {
                        IOrderedSet<IData, IData> data;
                        kv.Value.Lock.EnterWrite();
                        try { data = kv.Value.Split(leftRecordCount - halfRecordCount); }
                        finally { kv.Value.Lock.ExitWrite(); }
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
                    _container.Remove(kv.Key);

                foreach (var key in emptyContainers)
                {
                    if (_container.TryGetValue(key, out var empty))
                        NativeReclaim.Defer(empty);   // empty but still holds allocated native capacity
                    _container.Remove(key);
                }

                if (specialCase.Value != null) //have special case?
                    rightContainer[specialCase.Key] = specialCase.Value;
            }

            rightNode.RecordCount = RecordCount - leftRecordCount;
            RecordCount = leftRecordCount;
            rightNode.TouchId = TouchId;
            IsModified = true;

            return rightNode;
        }

        public override void Merge(Node node)
        {
            foreach (var kv in ((LeafNode)node)._container)
            {
                if (!_container.TryGetValue(kv.Key, out var data))
                    _container[kv.Key] = data = kv.Value;   // ADOPTED — lives on in this node, no reclaim
                else
                {
                    // Acquire write on target and READ on source.
                    // Without the source read lock a concurrent Fall→Apply on the source branch
                    // can acquire source.Lock.EnterWrite() (Merge holds no read lock on source)
                    // and mutate source._set while Merge iterates it → SortedSet corruption.
                    data.Lock.EnterWrite();
                    kv.Value.Lock.EnterRead();
                    try
                    {
                        RecordCount -= data.Count;
                        data.Merge(kv.Value);
                        RecordCount += data.Count;
                    }
                    finally
                    {
                        kv.Value.Lock.ExitRead();
                        data.Lock.ExitWrite();
                    }

                    // Source set's rows were copied into ours; the source is dropped with its node.
                    NativeReclaim.Defer(kv.Value);
                    continue;
                }

                RecordCount += data.Count;
            }

            if (TouchId < node.TouchId)
                TouchId = node.TouchId;

            // The source's rows (possibly still unflushed) live on in this node now — its recovery
            // boundary must stay pinned at least as far back, or a still-clean survivor (MinDirtyLsn ==
            // MaxValue) would stop constraining the incremental checkpoint LSN and silently drop ops that
            // only ever existed in the merged-away leaf's memory.
            if (node.MinDirtyLsn < MinDirtyLsn) MinDirtyLsn = node.MinDirtyLsn;
            if (node.PageLsn > PageLsn) PageLsn = node.PageLsn;

            IsModified = true;
        }

        /// <summary>Exact native bytes held by this leaf's ordered sets (slots + arena capacity). The
        /// byte-budget eviction adds this to its managed estimate — without it, native arenas are invisible
        /// to the budget and accumulate unbounded (measured 2.3 GB in 5 min before eviction ever fired).</summary>
        public override long NativeAllocatedBytes
        {
            get
            {
                long total = 0;
                foreach (var kv in _container)
                    if (kv.Value is NativeOrderedSet native)
                        total += native.AllocatedBytes;
                return total;
            }
        }

        /// <summary>Queues every container set for deferred native reclaim. Called when the node is
        /// unloaded from the cache (evicted after Store) — the sets are unreachable from the tree after
        /// that, and without this only the (lagging) finalizer would ever free their native memory.</summary>
        public override void ReleaseNativeData()
        {
            foreach (var kv in _container)
                NativeReclaim.Defer(kv.Value);
            _container.Clear();
        }

        public override bool IsOverflow => RecordCount > Branch.Tree._leafNodeMaxRecords;

        public override bool IsUnderflow
        {
            get
            {
                if (IsRoot)
                    return false;

                return RecordCount < Branch.Tree._leafNodeMinRecords;
            }
        }

        public override FullKey FirstKey
        {
            get
            {
                var kv = (_container.Count == 1) ? _container.First() : _container.OrderBy(x => x.Key).First();
                kv.Value.Lock.EnterRead();
                try { return new FullKey(kv.Key, kv.Value.First.Key); }
                finally { kv.Value.Lock.ExitRead(); }
            }
        }

        public override void Store(Stream stream)
        {
            var writer = new BinaryWriter(stream);
            writer.Write(VERSION);
            writer.Write(PageLsn); // v41: max op LSN reflected in this image (incremental-checkpoint redo-skip)

            CountCompression.Serialize(writer, checked((ulong)Branch.NodeHandle));

            CountCompression.Serialize(writer, checked((ulong)_container.Count));
            foreach (var kv in _container)
            {
                Branch.Tree.SerializeLocator(writer, kv.Key);
                kv.Key.OrderedSetPersist.Write(writer, kv.Value);
                // Reclaim over-allocated backing capacity now that the leaf is settled (post sink/split/delete).
                // Without this the List backing stays at its high-water capacity (e.g. 65536 slots ~7% full) —
                // a >85KB LOH array that wastes memory and churns the non-compacting LOH, escalating GC pause.
                kv.Value.TrimExcess();
            }

            IsModified = false;
            MinDirtyLsn = long.MaxValue; // clean: nothing unflushed
        }

        public override void Load(Stream stream)
        {
            var reader = new BinaryReader(stream);
            var version = reader.ReadByte();
            if (version != VERSION)
                throw new Exception("Invalid LeafNode version.");

            PageLsn = reader.ReadInt64();

            var id = (long)CountCompression.Deserialize(reader);
            if (id != Branch.NodeHandle)
                throw new Exception("Wtree logical error.");

            var count = (int)CountCompression.Deserialize(reader);
            for (var i = 0; i < count; i++)
            {
                var path = Branch.Tree.DeserializeLocator(reader);
                var data = path.OrderedSetPersist.Read(reader);
                _container[path] = data;

                RecordCount += data.Count;
            }

            IsModified = false;
        }

        public IOrderedSet<IData, IData> FindData(Locator locator, Direction direction, ref FullKey nearFullKey, ref bool hasNearFullKey)
        {
            IOrderedSet<IData, IData> data = null;
            _container.TryGetValue(locator, out data);
            if (direction == Direction.None)
                return data;

            if (_container.Count == 1 && data != null)
                return data;

            IOrderedSet<IData, IData> nearData = null;
            if (direction == Direction.Backward)
            {
                var havePrev = false;
                var prev = default(Locator);

                foreach (var kv in _container)
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
                    nearData.Lock.EnterRead();
                    try { nearFullKey = new FullKey(prev, nearData.Last.Key); }
                    finally { nearData.Lock.ExitRead(); }
                }
            }
            else //if (direction == Direction.Forward)
            {
                var haveNext = false;
                var next = default(Locator);

                foreach (var kv in _container)
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
                    nearData.Lock.EnterRead();
                    try { nearFullKey = new FullKey(next, nearData.First.Key); }
                    finally { nearData.Lock.ExitRead(); }
                }
            }

            return data;
        }
    }
}
