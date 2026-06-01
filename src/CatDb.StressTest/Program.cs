// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.StressTest;
using CatDb.Database;
using Database = CatDb.Database;

// ─── Parse command-line arguments ─────────────────────────────────────────
// Usage: dotnet run -- --duration 120   (runs for 120 seconds then stops)
//        dotnet run                     (runs until key press or Ctrl+C)

TimeSpan? maxDuration = null;
for (var i = 0; i < args.Length - 1; i++)
{
    if (args[i] is "--duration" or "-d" && int.TryParse(args[i + 1], out var seconds))
    {
        maxDuration = TimeSpan.FromSeconds(seconds);
        break;
    }
}

// ─── Bootstrap ────────────────────────────────────────────────────────────

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.Clear();
Console.CursorVisible = false;

// ── Switch between local file and remote server ───────────────────────────
const bool   USE_SERVER  = false;         // true = connect to CatDb.Server
const string SERVER_USERNAME = "admin";
const string SERVER_PASSWORD = "admin";
const string SERVER_HOST = "localhost";
const int    SERVER_PORT = 7182;
const string DB_FILE     = "catdb_stress.db";
// ─────────────────────────────────────────────────────────────────────────

#pragma warning disable CS0162 // const-bool branch — flip USE_SERVER to enable
IStorageEngine OpenEngine()
{
    if (USE_SERVER)
    {
        Console.WriteLine($"Connecting to server {SERVER_HOST}:{SERVER_PORT}...");
        return Database.CatDb.FromNetwork(
	        SERVER_HOST,
	        SERVER_PORT,
	        Path.GetFileNameWithoutExtension(DB_FILE),
	        SERVER_USERNAME,
	        SERVER_PASSWORD);
    }

    //if (File.Exists(DB_FILE)) File.Delete(DB_FILE);
    return Database.CatDb.FromFile(DB_FILE);
}
#pragma warning restore CS0162

Console.WriteLine("  Initializing CatDb Stress Test...");

using var ctx = new StressContext(OpenEngine());

// ─── Per-service connection (server mode) ─────────────────────────────────
// In server mode each service gets its own StressContext fork so it has its
// own dedicated TCP connection and table handles.  All forks share counters,
// the error log, and the CancellationToken via StressContext.SharedState.
// In file mode all services share the primary context (engine is thread-safe).

var forks = new List<StressContext>();

#pragma warning disable CS0162 // const-bool branch — flip USE_SERVER to enable
StressContext SvcCtx()
{
    if (!USE_SERVER) return ctx;
    var f = ctx.Fork(OpenEngine());
    forks.Add(f);
    return f;
}
#pragma warning restore CS0162

// ─── Create all background services ───────────────────────────────────────
//
//  Two TickIngest instances blast inserts at max speed (shared key counter).
//  Three Monkey instances do pure random chaos across every table.
//  Everything else runs a realistic domain workload concurrently.
//  CommitTimer flushes the engine to disk every 2 seconds.

var services = new List<BackgroundService>
{
    new TickIngestService     ("TickIngest-1",  SvcCtx()),
    new TickIngestService     ("TickIngest-2",  SvcCtx()),
    new SessionManagerService ("SessionMgr",    SvcCtx()),
    new OrderBookService      ("OrderBook",     SvcCtx()),
    new MetricsAggregatorService("MetricsAgg",  SvcCtx()),
    new AuditLogService       ("AuditLog",      SvcCtx()),
    new LeaderboardService    ("Leaderboard",   SvcCtx()),   // pre-populates 1 000 scores in ctor
    new BatchImportService    ("BatchImport",   SvcCtx()),
    new MonkeyService         ("Monkey-1",      SvcCtx()),
    new MonkeyService         ("Monkey-2",      SvcCtx()),
    new MonkeyService         ("Monkey-3",      SvcCtx()),
    new CommitService         ("CommitTimer",   SvcCtx(), intervalMs: 2_000),
    new KeySearchService      ("KeySearch",     SvcCtx()),
    new DataIntegrityService  ("DataIntegrity", SvcCtx()),
    new HighStressKeySearchService("HighSearch-A", SvcCtx(), wideMode: true),
    new HighStressKeySearchService("HighSearch-B", SvcCtx(), wideMode: false),
    new IndexStressService        ("IndexStress",  SvcCtx()),
};

// ─── Ctrl+C → graceful shutdown ───────────────────────────────────────────

Console.CancelKeyPress += (_, e) => { e.Cancel = true; ctx.Stop(); };

// ─── Auto-stop timer (if --duration was specified) ────────────────────────

if (maxDuration.HasValue)
{
    _ = Task.Delay(maxDuration.Value, CancellationToken.None).ContinueWith(_ => ctx.Stop());
}

// ─── Launch all services as long-running tasks ────────────────────────────
// Pure async — Task.Run schedules the async delegate without any
// .GetResult() / .Wait() bridging.

var tasks = services
    .Select(svc => Task.Run(() => svc.RunAsync(ctx.CancellationToken, ctx)))
    .ToArray();

// ─── Dashboard loop ───────────────────────────────────────────────────────

Console.Clear();
var dashboard = new Dashboard(services, ctx);

while (!ctx.CancellationToken.IsCancellationRequested)
{
    dashboard.Render();

    try   { await Task.Delay(400, ctx.CancellationToken); }
    catch (OperationCanceledException) { break; }

    if (Console.KeyAvailable)
    {
        Console.ReadKey(intercept: true);
        ctx.Stop();
        break;
    }
}

// ─── Shutdown ─────────────────────────────────────────────────────────────

Console.SetCursorPosition(0, 0);
Console.ForegroundColor = ConsoleColor.White;
Console.WriteLine(("  Stopping all services, please wait…").PadRight(92));
Console.ResetColor();

await Task.WhenAll(tasks);   // wait for every service to finish its current op
dashboard.WaitForRefresh();  // ensure no background count-refresh races engine dispose

ctx.Commit();                // final flush to disk

// Dispose per-service fork engines (own TCP connections in server mode)
foreach (var fork in forks)
    fork.Dispose();

Console.Clear();

#if PERFORMANCE_CHECK
CatDb.General.Diagnostics.PerformanceCheck.Flush("stress.final");
#endif

// ─── Final results ────────────────────────────────────────────────────────

dashboard.RenderFinal();
Console.CursorVisible = true;
