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
        private const byte VERSION = 40;

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
                data.Lock.EnterWrite();
                try
                {
                    RecordCount -= data.Count;

                    if (locator.Apply.Leaf(operations, data))
                        IsModified = true;

                    RecordCount += data.Count;

                    if (data.Count == 0)
                        _container.Remove(locator);
                }
                finally { data.Lock.ExitWrite(); }
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
                    _container.Remove(key);

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
                    _container[kv.Key] = data = kv.Value;
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
                    continue;
                }

                RecordCount += data.Count;
            }

            if (TouchId < node.TouchId)
                TouchId = node.TouchId;

            IsModified = true;
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

            CountCompression.Serialize(writer, checked((ulong)Branch.NodeHandle));

            CountCompression.Serialize(writer, checked((ulong)_container.Count));
            foreach (var kv in _container)
            {
                Branch.Tree.SerializeLocator(writer, kv.Key);
                kv.Key.OrderedSetPersist.Write(writer, kv.Value);
            }

            IsModified = false;
        }

        public override void Load(Stream stream)
        {
            var reader = new BinaryReader(stream);
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid LeafNode version.");

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
