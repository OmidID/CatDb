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
            writer.Write((int)FormatVersion.Current);

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
            writer.Write(tree._checkpointLsn); // TransactionLog recovery boundary
        }

        public static void Deserialize(WTree tree, Stream stream)
        {
            var reader  = new BinaryReader(stream);
            var version = reader.ReadInt32();

            if (version != FormatVersion.Current)
                throw new NotSupportedException(
                    $"Unsupported WTree header version {version}. CatDb v2 does not read pre-v2 " +
                    "database files (no STSDB/legacy compatibility) — recreate the database.");

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
            tree._checkpointLsn              = reader.ReadInt64();
        }
    }
}
