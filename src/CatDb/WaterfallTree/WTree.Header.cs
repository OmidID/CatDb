// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.WaterfallTree;

public partial class WTree
{
    private static class Settings
    {
        public static void Serialize(WTree tree, Stream stream)
        {
            var writer = new BinaryWriter(stream);
            const int version = 2;
            writer.Write(version);

            writer.Write(tree.GlobalVersion);
            writer.Write(tree._rootBranch.NodeHandle);
            writer.Write((byte)tree._rootBranch.NodeType);
            writer.Write(tree._depth);
            writer.Write(tree._internalNodeMinBranches);
            writer.Write(tree._internalNodeMaxBranches);
            writer.Write(tree._internalNodeMaxOperationsInRoot);
            writer.Write(tree._internalNodeMinOperations);
            writer.Write(tree._internalNodeMaxOperations);
            writer.Write(tree._leafNodeMinRecords);
            writer.Write(tree._leafNodeMaxRecords);
            writer.Write(tree._checkpointLsn); // v2: TransactionLog recovery boundary
        }

        public static void Deserialize(WTree tree, Stream stream)
        {
            var reader  = new BinaryReader(stream);
            var version = reader.ReadInt32();

            switch (version)
            {
                case 0:
                    tree.GlobalVersion               = reader.ReadInt64();
                    tree._rootBranch.NodeHandle      = reader.ReadInt64();
                    tree._rootBranch.NodeType        = (NodeType)reader.ReadByte();
                    tree._depth                      = reader.ReadInt32();
                    tree._internalNodeMaxOperationsInRoot = reader.ReadInt32();
                    tree._internalNodeMinBranches    = reader.ReadInt32();
                    tree._internalNodeMaxBranches    = reader.ReadInt32();
                    tree._internalNodeMinOperations  = reader.ReadInt32();
                    tree._internalNodeMaxOperations  = reader.ReadInt32();
                    break;

                case 1:
                case 2:
                    tree.GlobalVersion               = reader.ReadInt64();
                    tree._rootBranch.NodeHandle      = reader.ReadInt64();
                    tree._rootBranch.NodeType        = (NodeType)reader.ReadByte();
                    tree._depth                      = reader.ReadInt32();
                    tree._internalNodeMinBranches    = reader.ReadInt32();
                    tree._internalNodeMaxBranches    = reader.ReadInt32();
                    tree._internalNodeMaxOperationsInRoot = reader.ReadInt32();
                    tree._internalNodeMinOperations  = reader.ReadInt32();
                    tree._internalNodeMaxOperations  = reader.ReadInt32();
                    tree._leafNodeMinRecords         = reader.ReadInt32();
                    tree._leafNodeMaxRecords         = reader.ReadInt32();
                    tree._checkpointLsn              = version >= 2 ? reader.ReadInt64() : 0;
                    break;

                default:
                    throw new NotSupportedException("Unknown WTree header version.");
            }
        }
    }
}
