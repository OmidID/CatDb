using System.Collections.Concurrent;
using System.Diagnostics;
using CatDb.Database;

namespace CatDb.StressTest;

// ─── Shared context (one engine, many tables, global counters) ──────────────

public sealed class StressContext : IDisposable
{
    private readonly CancellationTokenSource _cts = new();

    public IStorageEngine Engine  { get; }
    public CancellationToken CancellationToken => _cts.Token;

    // Tables
    public ITable<long,   Tick>          Ticks    { get; }
    public ITable<string, UserSession>   Sessions { get; }
    public ITable<long,   Order>         Orders   { get; }
    public ITable<long,   MetricSnapshot>Metrics  { get; }
    public ITable<long,   AuditEntry>    Audit    { get; }
    public ITable<string, PlayerScore>   Scores   { get; }
    public ITable<long,   SensorReading> Sensors  { get; }
    public ITable<string, string>        Config   { get; }

    // Global sequence counters – use Interlocked.Increment / Volatile.Read
    public long NextTickId   = 0;
    public long NextOrderId  = 0;
    public long NextAuditSeq = 0;
    public long NextSensorId = 0;
    public long NextMetricId = 0;

    // Stats
    public long TotalCommits = 0;
    public long TotalErrors  = 0;
    public readonly ConcurrentQueue<(DateTime At, string Svc, string Msg)> RecentErrors = new();

    // Error log file
    public const string ErrorLogPath = "stress_errors.log";
    private readonly StreamWriter _errorLog;

    public StressContext(IStorageEngine engine)
    {
        // Delete stale log from a previous run and create a fresh one
        if (File.Exists(ErrorLogPath)) File.Delete(ErrorLogPath);
        _errorLog = new StreamWriter(ErrorLogPath, append: false) { AutoFlush = true };
        _errorLog.WriteLine($"CatDb Stress Test — Error Log — Started {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        _errorLog.WriteLine(new string('─', 80));

        Engine = engine;

        Ticks    = Engine.OpenXTable<long,   Tick>          ("ticks");
        Sessions = Engine.OpenXTable<string, UserSession>   ("sessions");
        Orders   = Engine.OpenXTable<long,   Order>         ("orders");
        Metrics  = Engine.OpenXTable<long,   MetricSnapshot>("metrics");
        Audit    = Engine.OpenXTable<long,   AuditEntry>    ("audit");
        Scores   = Engine.OpenXTable<string, PlayerScore>   ("scores");
        Sensors  = Engine.OpenXTable<long,   SensorReading> ("sensors");
        Config   = Engine.OpenXTable<string, string>        ("config");

        Config["app"]     = "CatDb.StressTest";
        Config["version"] = "1.0";
        Config["started"] = DateTime.UtcNow.ToString("O");
    }

    public void Commit()
    {
        Engine.Commit();
        Interlocked.Increment(ref TotalCommits);
    }

    public void Stop() => _cts.Cancel();

    public void RecordError(string svc, string msg, Exception? ex = null)
    {
        Interlocked.Increment(ref TotalErrors);
        var shortMsg = msg.Length > 100 ? msg[..100] : msg;
        RecentErrors.Enqueue((DateTime.Now, svc, shortMsg));
        while (RecentErrors.Count > 8)
            RecentErrors.TryDequeue(out _);

        // Write full details to the log file
        lock (_errorLog)
        {
            _errorLog.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{svc}] {msg}");
            if (ex != null)
            {
                _errorLog.WriteLine($"  Type   : {ex.GetType().FullName}");
                _errorLog.WriteLine($"  Stack  :");
                foreach (var line in (ex.StackTrace ?? "").Split('\n'))
                    _errorLog.WriteLine($"    {line.TrimEnd()}");
                if (ex.InnerException != null)
                {
                    _errorLog.WriteLine($"  InnerException: {ex.InnerException.GetType().FullName}: {ex.InnerException.Message}");
                    foreach (var line in (ex.InnerException.StackTrace ?? "").Split('\n'))
                        _errorLog.WriteLine($"    {line.TrimEnd()}");
                }
            }
            _errorLog.WriteLine();
        }
    }

    public void Dispose()
    {
        Engine.Dispose();
        _cts.Dispose();
        _errorLog.Flush();
        _errorLog.Dispose();
    }
}

// ─── Abstract background service ───────────────────────────────────────────

public abstract class BackgroundService
{
    public string Name    { get; }
    public long   TotalOps    = 0;
    public long   TotalErrors = 0;

    public volatile string LastOp    = "–";
    public volatile bool   IsRunning = false;

    // OpsPerSec stored as long bits to allow volatile; read via BitConverter
    private long _opsPerSecBits = 0;
    public double OpsPerSec
    {
        get => BitConverter.Int64BitsToDouble(Volatile.Read(ref _opsPerSecBits));
        private set => Volatile.Write(ref _opsPerSecBits, BitConverter.DoubleToInt64Bits(value));
    }

    private long              _windowOps = 0;
    private readonly Stopwatch _window   = Stopwatch.StartNew();

    private StressContext? _ctx;

    protected BackgroundService(string name) => Name = name;

    /// Record one successful operation and update rolling ops/sec.
    protected void Hit(string op)
    {
        Interlocked.Increment(ref TotalOps);
        Interlocked.Increment(ref _windowOps);
        LastOp = op;

        if (_window.Elapsed.TotalSeconds >= 1.0)
        {
            var secs = _window.Elapsed.TotalSeconds;
            var ops  = Interlocked.Exchange(ref _windowOps, 0);
            OpsPerSec = ops / secs;
            _window.Restart();
        }
    }

    protected void Fail(Exception ex, StressContext ctx)
    {
        Interlocked.Increment(ref TotalErrors);
        ctx.RecordError(Name, ex.Message, ex);
    }

    public async Task RunAsync(CancellationToken ct, StressContext ctx)
    {
        _ctx = ctx;
        IsRunning = true;
        try   { await ExecuteAsync(ct); }
        catch (OperationCanceledException) { /* graceful stop */ }
        catch (ObjectDisposedException) when (ct.IsCancellationRequested) { /* engine shutting down, graceful */ }
        catch (Exception ex) { Interlocked.Increment(ref TotalErrors); LastOp = $"FATAL: {ex.Message}"; ctx.RecordError(Name, $"FATAL: {ex.Message}", ex); }
        finally { IsRunning = false; }
    }

    protected abstract Task ExecuteAsync(CancellationToken ct);
}
