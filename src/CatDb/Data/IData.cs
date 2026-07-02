// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

// IData is now a global type alias for System.Object.
// This eliminates the per-operation Data<T> wrapper class that was causing ~400 MB/s
// gen0 allocation churn. Keys and records are stored directly as object in the engine:
//   - Reference types (class records): zero extra allocation — the reference is stored as-is
//   - Value types (long keys): one boxing allocation per key — same cost as before but simpler
// The Data<T> wrapper class is REMOVED from the hot path. SlotAccessor and IndexerPersist
// still compile typed expression trees; they now cast object directly to the concrete type.
global using IData = System.Object;
