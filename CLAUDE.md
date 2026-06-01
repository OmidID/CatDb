# CLAUDE.md

The agent instructions for this repo live in **[AGENTS.md](AGENTS.md)** — single source of truth
(architecture, build/test flow, concurrency model, critical invariants, debugging shortcuts).

@AGENTS.md

## Resolved — throughput decay (2026-06)

Symptom: after a few minutes of stress load, ops/sec dropped ~50%; restarting the process
(WITHOUT deleting the DB file) restored full speed. DB growth was never the cause.

Root cause: hot paths used `lock(this)`/`Monitor` on long-lived `Branch` objects, inflating their
CLR sync blocks — overhead accumulated in-process (restart cleared it; disk reload stayed fast).
Fix: dedicated `ReentrantLock` per Branch + all engine `lock()`/`Monitor` → `ReentrantLock`
(use `using (x.Lock())`). ~2x throughput (7.9k→14.4k ops/sec), 190/190 tests pass.

Lock rule (also in [AGENTS.md](AGENTS.md)): **ReentrantLock everywhere, never `lock()`/`Monitor.Enter/Exit`.**
Only `ReentrantLock.cs` may use `Monitor` internally.

Diagnosis notes for next time:
- Build flag `PERFORMANCE_CHECK` must be ON (`EnablePerformanceCheck=true` in
  [src/Directory.Build.props](src/Directory.Build.props)).
- `PerformanceCheck` flushes a fresh window every 20s and CLEARS it — each `[PERFORMANCE_CHECK]`
  block is an independent ~20s window, not cumulative. Diagnose decay by diffing an EARLY window
  against a LATE window; the metric whose avg/max climbs is the regression source.
- Headless stress run needs a pty or `Console.Clear` throws:
  `cd src/CatDb.StressTest && sleep 200 | script -q /dev/null dotnet run -c Release --no-build -- --duration 150`
