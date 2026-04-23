using CatDb.StressTest;
using CatDb.Database;
using Database = CatDb.Database;

// ─── Bootstrap ────────────────────────────────────────────────────────────

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.Clear();
Console.CursorVisible = false;

// ── Switch between local file and remote server ───────────────────────────
const bool   USE_SERVER  = false;         // true = connect to CatDb.Server
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
        return Database.CatDb.FromNetwork(SERVER_HOST, SERVER_PORT);
    }

    //if (File.Exists(DB_FILE)) File.Delete(DB_FILE);
    return Database.CatDb.FromFile(DB_FILE);
}
#pragma warning restore CS0162

Console.WriteLine("  Initializing CatDb Stress Test...");

using var ctx = new StressContext(OpenEngine());

// ─── Create all background services ───────────────────────────────────────
//
//  Two TickIngest instances blast inserts at max speed (shared key counter).
//  Three Monkey instances do pure random chaos across every table.
//  Everything else runs a realistic domain workload concurrently.
//  CommitTimer flushes the engine to disk every 2 seconds.

var services = new List<BackgroundService>
{
    new TickIngestService     ("TickIngest-1",  ctx),
    new TickIngestService     ("TickIngest-2",  ctx),
    new SessionManagerService ("SessionMgr",    ctx),
    new OrderBookService      ("OrderBook",     ctx),
    new MetricsAggregatorService("MetricsAgg",  ctx),
    new AuditLogService       ("AuditLog",      ctx),
    new LeaderboardService    ("Leaderboard",   ctx),   // pre-populates 1 000 scores in ctor
    new BatchImportService    ("BatchImport",   ctx),
    new MonkeyService         ("Monkey-1",      ctx),
    new MonkeyService         ("Monkey-2",      ctx),
    new MonkeyService         ("Monkey-3",      ctx),
    new CommitService         ("CommitTimer",   ctx, intervalMs: 2_000),
    new KeySearchService      ("KeySearch",     ctx),
    new DataIntegrityService  ("DataIntegrity", ctx),
    new HighStressKeySearchService("HighSearch-A", ctx, wideMode: true),
    new HighStressKeySearchService("HighSearch-B", ctx, wideMode: false),
};

// ─── Ctrl+C → graceful shutdown ───────────────────────────────────────────

Console.CancelKeyPress += (_, e) => { e.Cancel = true; ctx.Stop(); };

// ─── Launch all services as long-running tasks ────────────────────────────

var tasks = services
    .Select(svc => Task.Factory.StartNew(
        () => svc.RunAsync(ctx.CancellationToken, ctx).GetAwaiter().GetResult(),
        CancellationToken.None,
        TaskCreationOptions.LongRunning,
        TaskScheduler.Default))
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

// ─── Final results ────────────────────────────────────────────────────────

dashboard.RenderFinal();
Console.CursorVisible = true;
