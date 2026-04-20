using System.Diagnostics;
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
        Col(ConsoleColor.Magenta,
            $"  Commits: {Volatile.Read(ref _ctx.TotalCommits),4}   " +
            $"Total Ops: {totalOps,14:N0}   " +
            $"Total Errors: {totalErrs}\n");
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

        Console.Clear();
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
        Console.WriteLine($"  DB Commits   : {_ctx.TotalCommits}");
        Console.WriteLine($"  Total Errors : {totalErrs}");
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
