// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb;

/// <summary>
/// Single on-disk format version for every persisted structure in CatDb — WTree nodes, Locator,
/// Scheme, the WTree settings header, the operation log, indexer persisters, and delta compression.
/// Previously this was ~20 separate <c>VERSION = 40</c>/<c>41</c> literals scattered per file (a
/// legacy carried over from the STSDB-derived codebase). CatDb v2 collapses them into ONE constant
/// and is a clean break: no backward compatibility, no reading pre-v2/STSDB files. Bump
/// <see cref="Current"/> here — and only here — for the next breaking format change.
/// </summary>
public static class FormatVersion
{
    public const byte Current = 2;

    /// <summary>Discriminator for a native slotted-page leaf image (opt-in
    /// <see cref="Database.DatabaseOptions.UseNativeLeafStorage"/>), sharing the same version-byte
    /// slot as <see cref="Current"/> in <c>OrderedSetPersist</c>.</summary>
    public const byte NativeLeaf = 100;
}
