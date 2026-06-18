// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Text;
using System.Threading;

namespace CatDb.StressTest;

public sealed class Dashboard
{
    private readonly List<BackgroundService> _services;
    private readonly StressContext           _ctx;
    private readonly Stopwatch               _elapsed = Stopwatch.StartNew();

    // Table counts are refreshed asynchronously every ~3 s
    private readonly Dictionary<string, long> _counts        = new();
    private readonly Stopwatch                _countTimer    = Stopwatch.StartNew();
    private volatile bool                     _refreshing    = false;

    // Aggregate-throughput history for the live sparkline — one sample every CHART_SAMPLE_SECONDS,
    // measured from the true total-ops delta (not the per-service smoothed rate). Makes throughput
    // decay over a long run visible at a glance.
    private readonly List<double> _opsHistory   = new();
    private readonly List<double> _memHistory   = new();   // RSS (MB) per sample — side-by-side with OPS
    private readonly Stopwatch    _chartTimer   = Stopwatch.StartNew();
    private long                  _chartLastOps;
    private double                _opsPerSecNow;
    private double                _memNowMb;
    private const int             CHART_WIDTH    = 70;
    private const double          CHART_SAMPLE_SECONDS = 5.0;
    private static readonly char[] SparkBlocks = " ▁▂▃▄▅▆▇█".ToCharArray();
    private static readonly System.Diagnostics.Process Proc = System.Diagnostics.Process.GetCurrentProcess();

    private const int W = 92;   // console line width

    public Dashboard(List<BackgroundService> services, StressContext ctx)
    {
        _services = services;
        _ctx      = ctx;
        // Pre-fill with "?" so the first render shows something meaningful
        foreach (var t in new[] { "ticks","sessions","orders","sensors","audit","metrics","scores","config" })
            _counts[t] = -1;
    }

    // ─── Live render (called every ~400 ms) ──────────────────────────────────

    public void Render()
    {
        if (!_refreshing && _countTimer.Elapsed.TotalSeconds >= 3)
        {
            _refreshing = true;
            Task.Run(BackgroundRefreshCounts);
        }

        Console.SetCursorPosition(0, 0);

        var elapsed   = _elapsed.Elapsed;
        var totalOps  = _services.Sum(s => Volatile.Read(ref s.TotalOps));
        var totalErrs = _services.Sum(s => Volatile.Read(ref s.TotalErrors));

        // Sample aggregate throughput every ~CHART_SAMPLE_SECONDS for the sparkline.
        var dt = _chartTimer.Elapsed.TotalSeconds;
        if (dt >= CHART_SAMPLE_SECONDS)
        {
            _opsPerSecNow = (totalOps - _chartLastOps) / dt;
            _opsHistory.Add(_opsPerSecNow);
            if (_opsHistory.Count > CHART_WIDTH) _opsHistory.RemoveAt(0);
            _chartLastOps = totalOps;

            Proc.Refresh();
            _memNowMb = Proc.WorkingSet64 / (1024.0 * 1024);   // RSS — what the OS/Activity Monitor shows
            _memHistory.Add(_memNowMb);
            if (_memHistory.Count > CHART_WIDTH) _memHistory.RemoveAt(0);

            _chartTimer.Restart();
        }

        // Header
        Ln(ConsoleColor.Cyan,
            $"  CatDb Stress Test  ──  Running: {elapsed:hh\\:mm\\:ss}  ──  Press any key or Ctrl+C to stop");
        Sep();

        // Service table
        Col(ConsoleColor.DarkGray,
            $"  {"SERVICE",-20} {"OPS/SEC",10} {"TOTAL OPS",14} {"ERRORS",8}  {"LAST OPERATION",-40}\n");
        Sep();

        foreach (var svc in _services)
        {
            var ops  = Volatile.Read(ref svc.TotalOps);
            var errs = Volatile.Read(ref svc.TotalErrors);

            Col(svc.IsRunning ? ConsoleColor.Green : ConsoleColor.DarkGray, $"  {svc.Name,-20}");
            Col(ConsoleColor.Yellow, $" {svc.OpsPerSec,10:N0}");
            Col(ConsoleColor.White,  $" {ops,14:N0}");
            Col(errs > 0 ? ConsoleColor.Red : ConsoleColor.DarkGray, $" {errs,8:N0}");
            var lo = svc.LastOp ?? "";
            if (lo.Length > 40) lo = lo[..40] + "…";
            Col(ConsoleColor.DarkCyan, $"  {lo,-41}");
            Console.WriteLine();
        }

        Sep();

        // Table counts
        Col(ConsoleColor.Cyan, "  TABLE RECORD COUNTS  (refreshed every ~3 s)\n");
        Sep();
        PrintCounts(newline: true);
        Sep();

        // Totals
        var corruptions = Volatile.Read(ref _ctx.TotalCorruptions);
        var searchOps   = Volatile.Read(ref HighStressKeySearchService.TotalSearchOps);
        Col(ConsoleColor.Magenta,
            $"  Commits: {Volatile.Read(ref _ctx.TotalCommits),4}   " +
            $"Total Ops: {totalOps,14:N0}   " +
            $"Total Errors: {totalErrs}");
        Col(ConsoleColor.Cyan,
            $"  HighSearch Ops: {searchOps,12:N0}");
        if (corruptions > 0)
            Col(ConsoleColor.Red, $"   *** CORRUPTIONS: {corruptions} ***");
        Console.WriteLine();
        Sep();

        // Throughput + memory sparklines, aligned column-for-column (same sample cadence) so spikes and
        // GC/memory events line up visually — the point is to SEE whether perf dips correlate with memory.
        // Each scaled to its OWN peak. Two header lines + two bars, padded so they never wrap the layout.
        var opsPeak = _opsHistory.Count > 0 ? _opsHistory.Max() : 0;
        var memPeak = _memHistory.Count > 0 ? _memHistory.Max() : 0;
        var memMin  = _memHistory.Count > 0 ? _memHistory.Min() : 0;

        Col(ConsoleColor.Cyan, "  OPS/SEC ");
        Col(ConsoleColor.Yellow, $"{_opsPerSecNow,9:N0}");
        Col(ConsoleColor.DarkGray, $"  peak {opsPeak,9:N0}      ");
        Col(ConsoleColor.Cyan, "MEM(MB) ");
        Col(ConsoleColor.Magenta, $"{_memNowMb,7:N0}");
        Col(ConsoleColor.DarkGray, $"  {memMin,5:N0}-{memPeak,5:N0}");
        Console.WriteLine();
        Col(ConsoleColor.Green, Pad("  ops " + Spark(_opsHistory)));
        Console.WriteLine();
        // Memory bar scaled between its window min and max so the slow base-creep + sawtooth are visible
        // even when the absolute value barely changes.
        Col(ConsoleColor.Magenta, Pad("  mem " + SparkRange(_memHistory, memMin, memPeak)));
        Console.WriteLine();
        Sep();

        // Recent errors
        Col(ConsoleColor.Yellow, "  RECENT ERRORS\n");
        var errors = _ctx.RecentErrors.ToArray();
        if (errors.Length == 0)
        {
            Col(ConsoleColor.DarkGreen, Pad("  (none)") + "\n");
            for (int i = 1; i < 6; i++) Blank();
        }
        else
        {
            foreach (var (t, svc, msg) in errors.TakeLast(6))
                Col(ConsoleColor.Red, Pad($"  [{t:HH:mm:ss}] {svc}: {msg}") + "\n");
            for (int i = errors.Length; i < 6; i++) Blank();
        }

        Console.ResetColor();
    }

    /// Spin-wait until any fire-and-forget background count refresh completes.
    /// Must be called before the engine is disposed to avoid ObjectDisposedException.
    public void WaitForRefresh()
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (_refreshing && sw.Elapsed.TotalSeconds < 5)
            Thread.Sleep(10);
    }

    // ─── Final summary ────────────────────────────────────────────────────────

    public void RenderFinal()
    {
        // One last synchronous count refresh
        SyncRefreshCounts();

        var elapsed   = _elapsed.Elapsed;
        var totalOps  = _services.Sum(s => s.TotalOps);
        var totalErrs = _services.Sum(s => s.TotalErrors);
        var opsPerSec = elapsed.TotalSeconds > 0 ? totalOps / elapsed.TotalSeconds : 0;

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine();
        Console.WriteLine("  ╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("  ║            CatDb Stress Test  —  FINAL RESULTS                 ║");
        Console.WriteLine("  ╚══════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        Console.ForegroundColor = ConsoleColor.White;
        Console.WriteLine($"  Duration     : {elapsed:hh\\:mm\\:ss}");
        Console.WriteLine($"  Total Ops    : {totalOps:N0}");
        Console.WriteLine($"  Avg Ops/sec  : {opsPerSec:N0}");
        if (_opsHistory.Count > 1)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"  Throughput   : {Spark(_opsHistory)}  (peak {_opsHistory.Max():N0}/s)");
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine($"  Memory (RSS) : {SparkRange(_memHistory, _memHistory.Count > 0 ? _memHistory.Min() : 0, _memHistory.Count > 0 ? _memHistory.Max() : 0)}  ({(_memHistory.Count > 0 ? _memHistory.Min() : 0):N0}-{(_memHistory.Count > 0 ? _memHistory.Max() : 0):N0} MB, ~{CHART_SAMPLE_SECONDS:N0}s samples)");
            Console.ForegroundColor = ConsoleColor.White;
        }
        Console.WriteLine($"  DB Commits   : {_ctx.TotalCommits}");
        Console.WriteLine($"  Total Errors : {totalErrs}");
        var finalCorruptions = Volatile.Read(ref _ctx.TotalCorruptions);
        if (finalCorruptions > 0)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"  *** CORRUPTIONS : {finalCorruptions} ***  (check stress_errors.log)");
            Console.ForegroundColor = ConsoleColor.White;
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("  Data Integrity : ✓ No corruptions detected");
            Console.ForegroundColor = ConsoleColor.White;
        }
        Console.WriteLine();

        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"  {"SERVICE",-22} {"OPS",14}  {"ERRORS",8}");
        Console.WriteLine("  " + new string('─', 50));
        foreach (var s in _services.OrderByDescending(x => x.TotalOps))
        {
            Console.ForegroundColor = s.TotalErrors > 0 ? ConsoleColor.Red : ConsoleColor.Green;
            Console.WriteLine($"  {s.Name,-22} {s.TotalOps,14:N0}  {s.TotalErrors,8:N0}");
        }

        Console.WriteLine();
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("  FINAL TABLE RECORD COUNTS:");
        Console.ForegroundColor = ConsoleColor.White;
        foreach (var kv in _counts.OrderBy(x => x.Key))
            Console.WriteLine($"    {kv.Key,-12}  {(kv.Value < 0 ? "n/a" : kv.Value.ToString("N0"))}");

        Console.WriteLine();
        if (totalErrs == 0)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("  ✓  All services completed without errors — CatDb is solid!");
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"  ⚠  {totalErrs} error(s) recorded.");
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.WriteLine($"  →  Full details: {Path.GetFullPath(StressContext.ErrorLogPath)}");
        }

        Console.ResetColor();
        Console.WriteLine();
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private void PrintCounts(bool newline)
    {
        string F(string k) => _counts.TryGetValue(k, out var v) && v >= 0 ? v.ToString("N0") : "…";

        Col(ConsoleColor.White,
            $"  {"ticks",-10} {F("ticks"),14}   {"sessions",-10} {F("sessions"),14}   {"orders",-10} {F("orders"),14}\n");
        Col(ConsoleColor.White,
            $"  {"sensors",-10} {F("sensors"),14}   {"audit",-10} {F("audit"),14}   {"metrics",-10} {F("metrics"),14}\n");
        Col(ConsoleColor.White,
            $"  {"scores",-10} {F("scores"),14}   {"config",-10} {F("config"),14}\n");
    }

    private void BackgroundRefreshCounts()
    {
        try   { SyncRefreshCounts(); }
        finally
        {
            _countTimer.Restart();
            _refreshing = false;
        }
    }

    private void SyncRefreshCounts()
    {
        Count("ticks",    () => _ctx.Ticks.Count());
        Count("sessions", () => _ctx.Sessions.Count());
        Count("orders",   () => _ctx.Orders.Count());
        Count("sensors",  () => _ctx.Sensors.Count());
        Count("audit",    () => _ctx.Audit.Count());
        Count("metrics",  () => _ctx.Metrics.Count());
        Count("scores",   () => _ctx.Scores.Count());
        Count("config",   () => _ctx.Config.Count());
    }

    private void Count(string key, Func<long> fn)
    {
        try   { _counts[key] = fn(); }
        catch { /* ignore races during shutdown */ }
    }

    // Unicode block sparkline, each sample scaled relative to the window's peak.
    private static string Spark(List<double> history)
    {
        if (history.Count == 0) return "(collecting…)";
        var max = history.Max();
        if (max <= 0) return new string(SparkBlocks[0], history.Count);

        var sb = new StringBuilder(history.Count);
        foreach (var v in history)
        {
            var idx = (int)Math.Round(v / max * (SparkBlocks.Length - 1));
            if (idx < 0) idx = 0;
            else if (idx >= SparkBlocks.Length) idx = SparkBlocks.Length - 1;
            sb.Append(SparkBlocks[idx]);
        }
        return sb.ToString();
    }

    // Sparkline scaled between an explicit [min,max] window (not 0..max) — surfaces small relative
    // movement (slow base-memory creep, sawtooth) that a 0-based scale would flatten to a constant bar.
    private static string SparkRange(List<double> history, double min, double max)
    {
        if (history.Count == 0) return "(collecting…)";
        var span = max - min;
        if (span <= 0) return new string(SparkBlocks[1], history.Count);

        var sb = new StringBuilder(history.Count);
        foreach (var v in history)
        {
            var idx = (int)Math.Round((v - min) / span * (SparkBlocks.Length - 1));
            if (idx < 0) idx = 0;
            else if (idx >= SparkBlocks.Length) idx = SparkBlocks.Length - 1;
            sb.Append(SparkBlocks[idx]);
        }
        return sb.ToString();
    }

    private static void Sep() =>
        Col(ConsoleColor.DarkGray, "  " + new string('─', W - 4) + "\n");

    private static void Blank() =>
        Console.WriteLine(new string(' ', W));

    private static void Ln(ConsoleColor c, string text)
    {
        Console.ForegroundColor = c;
        Console.WriteLine(Pad(text));
    }

    private static void Col(ConsoleColor c, string text)
    {
        Console.ForegroundColor = c;
        Console.Write(text);
    }

    private static string Pad(string s) =>
        s.Length >= W ? s[..W] : s.PadRight(W);
}
