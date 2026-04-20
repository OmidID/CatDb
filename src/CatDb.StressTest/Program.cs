using CatDb.StressTest;

// ─── Bootstrap ────────────────────────────────────────────────────────────

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.Clear();
Console.CursorVisible = false;

const string DbFile = "catdb_stress.db";
if (File.Exists(DbFile)) File.Delete(DbFile);

Console.WriteLine("  Initializing CatDb Stress Test...");

using var ctx = new StressContext(DbFile);

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
