// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using CatDb.Database;

namespace CatDb.StressTest;

// ─── Shared context (one engine, many tables, global counters) ──────────────
//
// In server (network) mode every service gets its own StressContext fork so
// each one has its own dedicated TCP connection and table handles.  The
// SharedState inner class holds the counters, error log, and CancellationToken
// that must be the SAME object across every fork — no copying.
//
// In file mode all services share a single StressContext (unchanged behaviour).

public sealed class StressContext : IDisposable
{
    // ── Shared state: the same object for primary + all forks ──────────────

    private sealed class SharedState
    {
        // Global sequence counters — use Interlocked.Increment / Volatile.Read
        public long NextTickId;
        public long NextOrderId;
        public long NextAuditSeq;
        public long NextSensorId;
        public long NextMetricId;

        // Stats
        public long TotalCommits;
        public long TotalErrors;
        public long TotalCorruptions;

        public readonly ConcurrentQueue<(DateTime At, string Svc, string Msg)> RecentErrors = new();

        public readonly CancellationTokenSource Cts = new();

        private readonly StreamWriter _errorLog;

        public SharedState()
        {
            if (File.Exists(StressContext.ErrorLogPath)) File.Delete(StressContext.ErrorLogPath);
            _errorLog = new StreamWriter(StressContext.ErrorLogPath, append: false) { AutoFlush = true };
            _errorLog.WriteLine($"CatDb Stress Test — Error Log — Started {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            _errorLog.WriteLine(new string('─', 80));
        }

        public void RecordError(string svc, string msg, Exception? ex = null)
        {
            Interlocked.Increment(ref TotalErrors);
            var shortMsg = msg.Length > 100 ? msg[..100] : msg;
            RecentErrors.Enqueue((DateTime.Now, svc, shortMsg));
            while (RecentErrors.Count > 8)
                RecentErrors.TryDequeue(out _);

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
            Cts.Dispose();
            _errorLog.Flush();
            _errorLog.Dispose();
        }
    }

    // ── Per-context fields ─────────────────────────────────────────────────

    private readonly SharedState _s;
    private readonly bool _ownsShared; // only primary disposes SharedState

    public const string ErrorLogPath = "stress_errors.log";

    // ── Forwarded ref-returning properties (transparent to callers) ────────
    // Interlocked.Increment(ref ctx.NextTickId) and Volatile.Read(ref ctx.NextTickId)
    // work exactly as before because the property returns a ref to the shared field.

    public ref long NextTickId       => ref _s.NextTickId;
    public ref long NextOrderId      => ref _s.NextOrderId;
    public ref long NextAuditSeq     => ref _s.NextAuditSeq;
    public ref long NextSensorId     => ref _s.NextSensorId;
    public ref long NextMetricId     => ref _s.NextMetricId;
    public ref long TotalCommits     => ref _s.TotalCommits;
    public ref long TotalErrors      => ref _s.TotalErrors;
    public ref long TotalCorruptions => ref _s.TotalCorruptions;

    public ConcurrentQueue<(DateTime At, string Svc, string Msg)> RecentErrors
        => _s.RecentErrors;

    public CancellationToken CancellationToken => _s.Cts.Token;

    // ── Per-context: engine + table handles ───────────────────────────────

    public IStorageEngine Engine { get; }

    public ITable<long,   Tick>            Ticks     { get; }
    public ITable<string, UserSession>     Sessions  { get; }
    public ITable<long,   Order>           Orders    { get; }
    public ITable<long,   MetricSnapshot>  Metrics   { get; }
    public ITable<long,   AuditEntry>      Audit     { get; }
    public ITable<string, PlayerScore>     Scores    { get; }
    public ITable<long,   SensorReading>   Sensors   { get; }
    public ITable<string, string>          Config    { get; }
    public ITable<long,   IntegrityRecord> Integrity { get; }

    // ── Primary constructor ────────────────────────────────────────────────

    public StressContext(IStorageEngine engine)
    {
        _s = new SharedState();
        _ownsShared = true;
        Engine = engine;

        Ticks     = Engine.OpenXTable<long,   Tick>            ("ticks");
        Sessions  = Engine.OpenXTable<string, UserSession>     ("sessions");
        Orders    = Engine.OpenXTable<long,   Order>           ("orders");
        Metrics   = Engine.OpenXTable<long,   MetricSnapshot>  ("metrics");
        Audit     = Engine.OpenXTable<long,   AuditEntry>      ("audit");
        Scores    = Engine.OpenXTable<string, PlayerScore>     ("scores");
        Sensors   = Engine.OpenXTable<long,   SensorReading>   ("sensors");
        Config    = Engine.OpenXTable<string, string>          ("config");
        Integrity = Engine.OpenXTable<long,   IntegrityRecord> ("integrity");

        Config["app"]     = "CatDb.StressTest";
        Config["version"] = "1.0";
        Config["started"] = DateTime.UtcNow.ToString("O");
    }

    // ── Fork constructor ───────────────────────────────────────────────────
    // Shares SharedState (counters / log / CTS) with the primary but opens
    // its own table handles on a new engine — i.e. its own TCP connection.

    private StressContext(SharedState shared, IStorageEngine engine)
    {
        _s = shared;
        _ownsShared = false;
        Engine = engine;

        Ticks     = Engine.OpenXTable<long,   Tick>            ("ticks");
        Sessions  = Engine.OpenXTable<string, UserSession>     ("sessions");
        Orders    = Engine.OpenXTable<long,   Order>           ("orders");
        Metrics   = Engine.OpenXTable<long,   MetricSnapshot>  ("metrics");
        Audit     = Engine.OpenXTable<long,   AuditEntry>      ("audit");
        Scores    = Engine.OpenXTable<string, PlayerScore>     ("scores");
        Sensors   = Engine.OpenXTable<long,   SensorReading>   ("sensors");
        Config    = Engine.OpenXTable<string, string>          ("config");
        Integrity = Engine.OpenXTable<long,   IntegrityRecord> ("integrity");
    }

    /// <summary>
    /// Creates a fork that shares all counters/log/CTS with this context
    /// but uses <paramref name="engine"/> (its own TCP connection) for all I/O.
    /// Callers must dispose the returned context when done.
    /// </summary>
    public StressContext Fork(IStorageEngine engine) => new(_s, engine);

    // ── Methods ────────────────────────────────────────────────────────────

    public void Commit()
    {
        Engine.Commit();
        Interlocked.Increment(ref _s.TotalCommits);
    }

    public void Stop() => _s.Cts.Cancel();

    public void RecordError(string svc, string msg, Exception? ex = null) =>
        _s.RecordError(svc, msg, ex);

    public void Dispose()
    {
        Engine.Dispose();
        if (_ownsShared) _s.Dispose();
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
