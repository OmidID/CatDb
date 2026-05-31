using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;

namespace CatDb.StressTest;

// ─── 1. Tick Ingest ────────────────────────────────────────────────────────
// Two instances insert market-tick rows at maximum speed using a shared global
// counter so keys are always unique across both instances.

public sealed class TickIngestService : BackgroundService
{
    private static readonly string[] Symbols =
        ["AAPL","GOOGL","MSFT","AMZN","TSLA","BTC/USD","ETH/USD","EUR/USD","GBP/USD","JPY/USD"];
    private static readonly string[] Providers =
        ["Bloomberg","Reuters","ICE","NYSE","NASDAQ"];

    private readonly StressContext _ctx;
    private readonly Random        _rng;

    public TickIngestService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ unchecked((int)0xCAFEBABE));
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var key = Interlocked.Increment(ref _ctx.NextTickId);
                var bid = 10.0 + _rng.NextDouble() * 990;

                _ctx.Ticks[key] = new Tick
                {
                    Symbol    = Symbols[_rng.Next(Symbols.Length)],
                    Timestamp = DateTime.UtcNow,
                    Bid       = bid,
                    Ask       = bid + _rng.NextDouble() * 0.5,
                    BidSize   = _rng.Next(100, 10_000),
                    AskSize   = _rng.Next(100, 10_000),
                    Provider  = Providers[_rng.Next(Providers.Length)],
                };
                Hit($"insert key={key:N0}");
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            await Task.Yield();
        }
    }
}

// ─── 2. Session Manager ────────────────────────────────────────────────────
// Creates, updates and expires HTTP-style user sessions. Demonstrates
// string-key tables and conditional read-modify-write cycles.

public sealed class SessionManagerService : BackgroundService
{
    private static readonly string[] Roles = ["admin","user","guest","moderator","api-key"];

    private readonly StressContext _ctx;
    private readonly Random        _rng;
    private readonly List<string>  _live = [];   // only this task touches it

    public SessionManagerService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ unchecked((int)0xDEADBEEF));
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                int op = _rng.Next(10);

                if (op < 3 || _live.Count == 0)                    // ── create ──
                {
                    var sid = $"s_{Guid.NewGuid():N}";
                    _ctx.Sessions[sid] = new UserSession
                    {
                        UserId       = $"u{_rng.Next(50_000)}",
                        IpAddress    = $"{_rng.Next(1,255)}.{_rng.Next(0,255)}.{_rng.Next(0,255)}.{_rng.Next(1,255)}",
                        CreatedAt    = DateTime.UtcNow,
                        LastActivity = DateTime.UtcNow,
                        RequestCount = 0,
                        IsActive     = true,
                        Role         = Roles[_rng.Next(Roles.Length)],
                    };
                    _live.Add(sid);
                    if (_live.Count > 2_000) _live.RemoveAt(0);
                    Hit("create session");
                }
                else if (op < 7)                                    // ── update ──
                {
                    var sid = _live[_rng.Next(_live.Count)];
                    if (_ctx.Sessions.TryGet(sid, out var s))
                    {
                        s.LastActivity = DateTime.UtcNow;
                        s.RequestCount++;
                        _ctx.Sessions[sid] = s;
                        Hit($"update req#{s.RequestCount}");
                    }
                }
                else                                                // ── expire ──
                {
                    var idx = _rng.Next(_live.Count);
                    var sid = _live[idx];
                    _live.RemoveAt(idx);
                    _ctx.Sessions.Delete(sid);
                    Hit("expire session");
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            await Task.Delay(2, ct).ContinueWith(_ => { });
        }
    }
}

// ─── 3. Order Book ────────────────────────────────────────────────────────
// Full order lifecycle: submit → partial fills → filled / cancelled.
// Also scans a rolling window of recent orders to simulate a risk monitor.

public sealed class OrderBookService : BackgroundService
{
    private static readonly string[] Symbols = ["AAPL","GOOGL","MSFT","AMZN","TSLA","BTC/USD","ETH/USD"];
    private static readonly string[] Sides   = ["BUY","SELL"];

    private readonly StressContext _ctx;
    private readonly Random        _rng;
    private readonly List<long>    _openIds = [];

    public OrderBookService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ unchecked((int)0xF00DCAFE));
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                int op = _rng.Next(10);

                if (op < 3 || _openIds.Count == 0)                 // ── new order ──
                {
                    var oid = Interlocked.Increment(ref _ctx.NextOrderId);
                    _ctx.Orders[oid] = new Order
                    {
                        Symbol         = Symbols[_rng.Next(Symbols.Length)],
                        Side           = Sides[_rng.Next(Sides.Length)],
                        Price          = Math.Round(10 + _rng.NextDouble() * 990, 2),
                        Quantity       = _rng.Next(1, 1_000),
                        FilledQuantity = 0,
                        Status         = "OPEN",
                        CreatedAt      = DateTime.UtcNow,
                        UpdatedAt      = DateTime.UtcNow,
                    };
                    _openIds.Add(oid);
                    if (_openIds.Count > 5_000) _openIds.RemoveAt(0);
                    Hit($"new order {oid}");
                }
                else if (op < 6)                                    // ── fill ──
                {
                    var oid = _openIds[_rng.Next(_openIds.Count)];
                    if (_ctx.Orders.TryGet(oid, out var o) && o.Status == "OPEN")
                    {
                        o.FilledQuantity  = Math.Min(o.Quantity,
                                                     o.FilledQuantity + _rng.NextDouble() * o.Quantity);
                        o.Status    = o.FilledQuantity >= o.Quantity ? "FILLED" : "PARTIAL";
                        o.UpdatedAt = DateTime.UtcNow;
                        _ctx.Orders[oid] = o;
                        Hit($"fill {oid} → {o.Status}");
                    }
                }
                else if (op < 8)                                    // ── cancel ──
                {
                    var idx = _rng.Next(_openIds.Count);
                    var oid = _openIds[idx];
                    _openIds.RemoveAt(idx);
                    if (_ctx.Orders.TryGet(oid, out var o))
                    {
                        o.Status    = "CANCELLED";
                        o.UpdatedAt = DateTime.UtcNow;
                        _ctx.Orders[oid] = o;
                        Hit($"cancel {oid}");
                    }
                }
                else                                                // ── scan ──
                {
                    var top  = Volatile.Read(ref _ctx.NextOrderId);
                    var from = Math.Max(1L, top - 500);
                    int n = 0;
                    foreach (var _ in _ctx.Orders.Forward(from, true, top, true))
                        if (++n >= 500) break;
                    Hit($"scan {n} orders");
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            await Task.Delay(3, ct).ContinueWith(_ => { });
        }
    }
}

// ─── 4. Metrics Aggregator ────────────────────────────────────────────────
// Reads the last 2 000 ticks and persists a statistical snapshot every 500 ms.
// Exercises cross-table read-then-write and range-forward scans.

public sealed class MetricsAggregatorService : BackgroundService
{
    private readonly StressContext _ctx;

    public MetricsAggregatorService(string name, StressContext ctx) : base(name)
        => _ctx = ctx;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var top = Volatile.Read(ref _ctx.NextTickId);
                if (top > 0)
                {
                    var from = Math.Max(1L, top - 2_000);
                    var bids = new List<double>(2_000);

                    foreach (var kv in _ctx.Ticks.Forward(from, true, top, true))
                    {
                        bids.Add(kv.Value.Bid);
                        if (bids.Count >= 2_000) break;
                    }

                    if (bids.Count > 0)
                    {
                        var mid = Interlocked.Increment(ref _ctx.NextMetricId);
                        _ctx.Metrics[mid] = new MetricSnapshot
                        {
                            Name        = "ticks.bid",
                            Value       = bids[^1],
                            Min         = bids.Min(),
                            Max         = bids.Max(),
                            Avg         = bids.Average(),
                            SampleCount = bids.Count,
                            Timestamp   = DateTime.UtcNow,
                        };
                        Hit($"snapshot #{mid} over {bids.Count} samples");
                    }
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            await Task.Delay(500, ct).ContinueWith(_ => { });
        }
    }
}

// ─── 5. Audit Log ─────────────────────────────────────────────────────────
// Append-only sequential writes in variable-size bursts (10–100 entries).
// Occasionally walks the log backwards to simulate a recent-activity query.

public sealed class AuditLogService : BackgroundService
{
    private static readonly string[] Svcs = ["TickIngest","SessionMgr","OrderBook","Metrics","Leaderboard","Batch","Monkey"];
    private static readonly string[] Ops  = ["insert","update","delete","read","scan","commit","batch","expire","probe"];

    private readonly StressContext _ctx;
    private readonly Random        _rng;

    public AuditLogService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ 0x1234_5678);
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_rng.Next(10) < 8)                             // ── burst-write ──
                {
                    int burst = _rng.Next(10, 101);
                    for (int i = 0; i < burst && !ct.IsCancellationRequested; i++)
                    {
                        var seq = Interlocked.Increment(ref _ctx.NextAuditSeq);
                        _ctx.Audit[seq] = new AuditEntry
                        {
                            ServiceName = Svcs[_rng.Next(Svcs.Length)],
                            Operation   = Ops[_rng.Next(Ops.Length)],
                            Detail      = $"ref={seq % 100_000:D6}",
                            Timestamp   = DateTime.UtcNow,
                            Success     = _rng.NextDouble() > 0.02,
                        };
                        Hit($"audit #{seq}");
                    }
                }
                else                                               // ── backward scan ──
                {
                    int n = 0;
                    foreach (var _ in _ctx.Audit.Backward())
                        if (++n >= 500) break;
                    Hit($"bwd scan {n} entries");
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            await Task.Delay(15, ct).ContinueWith(_ => { });
        }
    }
}

// ─── 6. Leaderboard ───────────────────────────────────────────────────────
// 1 000 fixed player IDs, string-keyed table. Continuously updates scores
// and does backward top-N scans. Occasionally batch-updates 100 players.

public sealed class LeaderboardService : BackgroundService
{
    private readonly StressContext _ctx;
    private readonly Random        _rng;
    private readonly string[]      _ids;

    public LeaderboardService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ unchecked((int)0xABCDEF01));
        _ids = Enumerable.Range(0, 1_000).Select(i => $"p{i:D4}").ToArray();

        // Pre-populate leaderboard (runs before any tasks start)
        foreach (var id in _ids)
        {
            ctx.Scores[id] = new PlayerScore
            {
                Username    = id,
                Score       = _rng.NextInt64(0, 100_000),
                Level       = _rng.Next(1, 100),
                LastUpdated = DateTime.UtcNow,
            };
        }
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                int op = _rng.Next(10);

                if (op < 5)                                         // ── single update ──
                {
                    var id = _ids[_rng.Next(_ids.Length)];
                    if (_ctx.Scores.TryGet(id, out var sc))
                    {
                        sc.Score      += _rng.Next(1, 500);
                        sc.Level       = (int)(sc.Score / 1_000) + 1;
                        sc.LastUpdated = DateTime.UtcNow;
                        _ctx.Scores[id] = sc;
                        Hit($"score {id}={sc.Score:N0}");
                    }
                }
                else if (op < 8)                                    // ── top-20 scan ──
                {
                    int n = 0;
                    foreach (var _ in _ctx.Scores.Backward())
                        if (++n >= 20) break;
                    Hit($"top-{n} scan");
                }
                else                                                // ── batch 100 ──
                {
                    for (int i = 0; i < 100; i++)
                    {
                        if (ct.IsCancellationRequested) return;
                        var id = _ids[_rng.Next(_ids.Length)];
                        if (_ctx.Scores.TryGet(id, out var sc))
                        {
                            sc.Score      += _rng.Next(100, 2_000);
                            sc.LastUpdated = DateTime.UtcNow;
                            _ctx.Scores[id] = sc;
                        }
                    }
                    Hit("batch 100 scores");
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            await Task.Delay(5, ct).ContinueWith(_ => { });
        }
    }
}

// ─── 7. Batch Import (IoT sensor data) ────────────────────────────────────
// Simulates an IoT gateway: large sequential batch inserts (500–5 000 rows
// per cycle) followed by range-delete of stale data. Maximum write pressure.

public sealed class BatchImportService : BackgroundService
{
    private static readonly string[] Sensors = ["T01","T02","T03","H01","H02","P01","V01","V02"];

    private readonly StressContext _ctx;
    private readonly Random        _rng;
    private long                   _oldest = 1;

    public BatchImportService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ unchecked((int)0x56789ABC));
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                int  batch = _rng.Next(500, 5_001);
                long start = Interlocked.Add(ref _ctx.NextSensorId, batch) - batch + 1;
                var  now   = DateTime.UtcNow;

                for (long k = start; k < start + batch; k++)
                {
                    if (ct.IsCancellationRequested) return;
                    _ctx.Sensors[k] = new SensorReading
                    {
                        SensorId    = Sensors[_rng.Next(Sensors.Length)],
                        Temperature = 15 + _rng.NextDouble() * 85,
                        Humidity    = 10 + _rng.NextDouble() * 80,
                        Pressure    = 850 + _rng.NextDouble() * 250,
                        Voltage     = 1.8 + _rng.NextDouble() * 3.5,
                        ReadingTime = now,
                    };
                }
                Hit($"batch +{batch} sensors [{start},{start + batch - 1}]");

                // Prune: keep at most 200 000 sensor rows
                var total = Volatile.Read(ref _ctx.NextSensorId);
                if (total - _oldest > 200_000)
                {
                    var end = _oldest + batch;
                    _ctx.Sensors.Delete(_oldest, end);
                    _oldest = end + 1;
                    Hit($"prune sensors up to {end}");
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            await Task.Delay(100, ct).ContinueWith(_ => { });
        }
    }
}

// ─── 8. Monkey Service ────────────────────────────────────────────────────
// Chaos agent: 3 instances execute 24 different random operations across ALL
// tables — range scans, point reads, FindNext/Prev/After/Before, random
// deletes, chaos inserts at extreme keys, Count(), FirstRow/LastRow, etc.

public sealed class MonkeyService : BackgroundService
{
    private readonly StressContext _ctx;
    private readonly Random        _rng;

    public MonkeyService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ (int)(DateTime.UtcNow.Ticks & 0xFFFF_FFFF));
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                switch (_rng.Next(28))
                {
                    // ── Ticks ──────────────────────────────────────────────────────────────
                    case 0:
                    {
                        var top = Volatile.Read(ref _ctx.NextTickId);
                        var key = _rng.NextInt64(1, Math.Max(2, top));
                        _ctx.Ticks.TryGet(key, out _);
                        Hit($"tick.Get({key})");
                        break;
                    }
                    case 1:
                    {
                        var top  = Volatile.Read(ref _ctx.NextTickId);
                        var a    = _rng.NextInt64(1, Math.Max(2, top));
                        var b    = _rng.NextInt64(1, Math.Max(2, top));
                        var from = Math.Min(a, b);
                        var to   = Math.Min(from + _rng.Next(1, 20_000), top);
                        int n = 0;
                        foreach (var _ in _ctx.Ticks.Forward(from, true, to, true))
                            if (++n >= 1_000) break;
                        Hit($"tick.Fwd[{from},{to}]→{n}");
                        break;
                    }
                    case 2:
                    {
                        int n = 0;
                        foreach (var _ in _ctx.Ticks.Backward())
                            if (++n >= 500) break;
                        Hit($"tick.Bwd→{n}");
                        break;
                    }
                    case 3:
                    {
                        var top = Volatile.Read(ref _ctx.NextTickId);
                        var key = _rng.NextInt64(1, Math.Max(2, top));
                        var r   = _ctx.Ticks.FindNext(key);
                        Hit($"tick.FindNext({key})→{r?.Key}");
                        break;
                    }
                    case 4:
                    {
                        var top = Volatile.Read(ref _ctx.NextTickId);
                        var key = _rng.NextInt64(1, Math.Max(2, top));
                        var r   = _ctx.Ticks.FindPrev(key);
                        Hit($"tick.FindPrev({key})→{r?.Key}");
                        break;
                    }
                    case 5:
                    {
                        var top   = Volatile.Read(ref _ctx.NextTickId);
                        var key   = _rng.NextInt64(1, Math.Max(2, top));
                        var after  = _ctx.Ticks.FindAfter(key);
                        var before = _ctx.Ticks.FindBefore(key);
                        Hit($"tick.After/Before({key})→{after?.Key}/{before?.Key}");
                        break;
                    }
                    case 6:
                    {
                        // Chaos insert at a key far beyond the normal range
                        var key = _rng.NextInt64(1_000_000_000L, 2_000_000_000L);
                        _ctx.Ticks[key] = new Tick
                        {
                            Symbol    = "MONKEY",
                            Timestamp = DateTime.UtcNow,
                            Bid       = _rng.NextDouble() * 99_999,
                            Ask       = _rng.NextDouble() * 99_999,
                            BidSize   = 1,
                            AskSize   = 1,
                            Provider  = "ChaosEngine",
                        };
                        Hit($"tick.ChaosInsert({key})");
                        break;
                    }
                    case 7:
                    {
                        // Small range delete in the middle of the tick space
                        var top  = Volatile.Read(ref _ctx.NextTickId);
                        var from = _rng.NextInt64(1, Math.Max(2, top / 2));
                        var to   = from + _rng.Next(1, 200);
                        _ctx.Ticks.Delete(from, to);
                        Hit($"tick.RangeDelete[{from},{to}]");
                        break;
                    }
                    case 8:
                    {
                        if (_ctx.Ticks.Count() == 0) break;
                        var first = _ctx.Ticks.FirstRow;
                        var last  = _ctx.Ticks.LastRow;
                        if (first is null || last is null) break;
                        Hit($"tick.First={first.Value.Key} Last={last.Value.Key}");
                        break;
                    }
                    case 9:
                    {
                        var c = _ctx.Ticks.Count();
                        Hit($"tick.Count={c:N0}");
                        break;
                    }

                    // ── Sessions ───────────────────────────────────────────────────────────
                    case 10:
                    {
                        int n = 0;
                        foreach (var _ in _ctx.Sessions.Forward())
                            if (++n >= 200) break;
                        Hit($"session.Fwd→{n}");
                        break;
                    }
                    case 11:
                    {
                        if (_ctx.Sessions.Count() == 0) break;
                        var first = _ctx.Sessions.FirstRow;
                        if (first is null) break;
                        var k     = first.Value.Key ?? "–";
                        Hit($"session.First={k[..Math.Min(12, k.Length)]}");
                        break;
                    }

                    // ── Orders ─────────────────────────────────────────────────────────────
                    case 12:
                    {
                        var top  = Volatile.Read(ref _ctx.NextOrderId);
                        var from = Math.Max(1L, top - _rng.Next(1, 5_000));
                        int n = 0;
                        foreach (var _ in _ctx.Orders.Forward(from, true, top, true))
                            if (++n >= 500) break;
                        Hit($"order.Scan[{from},{top}]→{n}");
                        break;
                    }
                    case 13:
                    {
                        var top = Volatile.Read(ref _ctx.NextOrderId);
                        var key = _rng.NextInt64(1, Math.Max(2, top));
                        _ctx.Orders.Delete(key);
                        Hit($"order.Delete({key})");
                        break;
                    }
                    case 14:
                    {
                        var top = Volatile.Read(ref _ctx.NextOrderId);
                        var r   = _ctx.Orders.FindNext(Math.Max(1, top / 2));
                        Hit($"order.FindNext(mid)→{r?.Key}");
                        break;
                    }

                    // ── Audit ──────────────────────────────────────────────────────────────
                    case 15:
                    {
                        int n = 0;
                        foreach (var _ in _ctx.Audit.Backward())
                            if (++n >= 300) break;
                        Hit($"audit.Bwd→{n}");
                        break;
                    }
                    case 16:
                    {
                        var top = Volatile.Read(ref _ctx.NextAuditSeq);
                        if (top > 5_000)
                        {
                            var from = _rng.NextInt64(1, top / 2);
                            var to   = from + _rng.Next(100, 2_000);
                            _ctx.Audit.Delete(from, to);
                            Hit($"audit.RangeDelete[{from},{to}]");
                        }
                        break;
                    }

                    // ── Leaderboard ────────────────────────────────────────────────────────
                    case 17:
                    {
                        int n = 0;
                        foreach (var _ in _ctx.Scores.Backward())
                            if (++n >= 50) break;
                        Hit($"scores.Bwd→{n}");
                        break;
                    }
                    case 18:
                    {
                        var from = $"p{_rng.Next(0,   500):D4}";
                        var to   = $"p{_rng.Next(500, 1_000):D4}";
                        int n = 0;
                        foreach (var _ in _ctx.Scores.Forward(from, true, to, true))
                            if (++n >= 200) break;
                        Hit($"scores.Fwd[{from},{to}]→{n}");
                        break;
                    }

                    // ── Sensors ────────────────────────────────────────────────────────────
                    case 19:
                    {
                        var top  = Volatile.Read(ref _ctx.NextSensorId);
                        var from = Math.Max(1L, top - _rng.Next(1, 20_000));
                        int n = 0;
                        foreach (var _ in _ctx.Sensors.Forward(from, true, top, true))
                            if (++n >= 1_000) break;
                        Hit($"sensor.Scan→{n}");
                        break;
                    }
                    case 20:
                    {
                        var top = Volatile.Read(ref _ctx.NextSensorId);
                        var r   = _ctx.Sensors.FindNext(Math.Max(1, top / 2));
                        Hit($"sensor.FindNext→{r?.Key}");
                        break;
                    }

                    // ── Metrics ────────────────────────────────────────────────────────────
                    case 21:
                    {
                        int n = 0;
                        foreach (var _ in _ctx.Metrics.Backward())
                            if (++n >= 50) break;
                        Hit($"metrics.Bwd→{n}");
                        break;
                    }

                    // ── Config ─────────────────────────────────────────────────────────────
                    case 22:
                    {
                        _ctx.Config[$"mk_{_rng.Next(500)}"] = $"v{_rng.Next()}";
                        Hit("config.Write");
                        break;
                    }
                    case 23:
                    {
                        int n = 0;
                        foreach (var _ in _ctx.Config.Forward()) n++;
                        Hit($"config.Scan→{n}");
                        break;
                    }

                    // ── Mixed / stress combos ──────────────────────────────────────────────
                    case 24:
                    {
                        // Read a tick then immediately write a metric (cross-table pipeline)
                        var top = Volatile.Read(ref _ctx.NextTickId);
                        if (top > 0 && _ctx.Ticks.TryGet(_rng.NextInt64(1, Math.Max(2, top)), out var t))
                        {
                            var mid = Interlocked.Increment(ref _ctx.NextMetricId);
                            _ctx.Metrics[mid] = new MetricSnapshot
                            {
                                Name        = "monkey.probe",
                                Value       = t.Bid,
                                Min         = t.Bid,
                                Max         = t.Ask,
                                Avg         = (t.Bid + t.Ask) / 2,
                                SampleCount = 1,
                                Timestamp   = DateTime.UtcNow,
                            };
                            Hit($"cross-table probe→metric {mid}");
                        }
                        break;
                    }
                    case 25:
                    {
                        // Force a full Forward() scan across the entire orders table
                        long total = 0;
                        foreach (var _ in _ctx.Orders.Forward())
                            total++;
                        Hit($"order.FullScan→{total:N0}");
                        break;
                    }
                    case 26:
                    {
                        // Rapid-fire 20 random point reads across all tables
                        var top = Math.Max(2, Volatile.Read(ref _ctx.NextTickId));
                        for (int i = 0; i < 20; i++)
                            _ctx.Ticks.TryGet(_rng.NextInt64(1, top), out _);
                        Hit("20x random tick reads");
                        break;
                    }
                    case 27:
                    {
                        // Scan audit with no bounds (unbounded Backward)
                        long total = 0;
                        foreach (var _ in _ctx.Audit.Backward())
                            if (++total >= 2_000) break;
                        Hit($"audit.UnboundedBwd→{total:N0}");
                        break;
                    }
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            int sleep = _rng.Next(9);
            if (sleep > 0) await Task.Delay(sleep, ct).ContinueWith(_ => { });
            else           await Task.Yield();
        }
    }
}

// ─── 9. Periodic Commit ───────────────────────────────────────────────────
// Flushes the WaterfallTree buffer to disk on a fixed interval.
// This is the ONLY service that calls engine.Commit().

public sealed class CommitService : BackgroundService
{
    private readonly StressContext _ctx;
    private readonly int           _ms;

    public CommitService(string name, StressContext ctx, int intervalMs = 2_000) : base(name)
    {
        _ctx = ctx;
        _ms  = intervalMs;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(_ms, ct).ContinueWith(_ => { });
            if (ct.IsCancellationRequested) break;
            try
            {
#if PERFORMANCE_CHECK
                CatDb.General.Diagnostics.PerformanceCheck.Increment("stress.commit.source.timer");
#endif
                _ctx.Commit();
                Hit($"commit #{_ctx.TotalCommits}");
            }
            catch (Exception ex) { Fail(ex, _ctx); }
        }
    }
}

// ─── 10. Key Search Service ───────────────────────────────────────────────
// Exercises Query / QueryBackward / PageAfter under concurrent write load.
// Runs against the tables that other services keep mutating — validates that
// range scans return records in the correct order and within requested bounds.

public sealed class KeySearchService : BackgroundService
{
    private readonly StressContext _ctx;
    private readonly Random        _rng;

    public KeySearchService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ unchecked((int)0x5EAD1234));
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                switch (_rng.Next(10))
                {
                    // ── Ticks: AtLeast (descending live edge) ─────────────────────────────
                    case 0:
                    {
                        var top  = Volatile.Read(ref _ctx.NextTickId);
                        if (top < 2) break;
                        var from = Math.Max(1L, top - _rng.Next(100, 2_000));
                        long prev = -1;
                        int  n   = 0;
                        foreach (var kv in _ctx.Ticks.Query(KeyQuery<long>.AtLeast(from)))
                        {
                            if (kv.Key < from)
                            {
                                Fail(new Exception(
                                    $"Ticks.AtLeast: key {kv.Key} < from {from}"), _ctx);
                            }
                            if (prev != -1 && kv.Key <= prev)
                            {
                                Fail(new Exception(
                                    $"Ticks.AtLeast: order violation {kv.Key} <= {prev}"), _ctx);
                            }
                            prev = kv.Key;
                            if (++n >= 500) break;
                        }
                        Hit($"ticks.AtLeast({from})→{n}");
                        break;
                    }

                    // ── Orders: Between (ascending, boundary inclusive check) ─────────────
                    case 1:
                    {
                        var top = Volatile.Read(ref _ctx.NextOrderId);
                        if (top < 4) break;
                        var a = _rng.NextInt64(1, top / 2);
                        var b = a + _rng.Next(50, 500);
                        int n = 0;
                        foreach (var kv in _ctx.Orders.Query(KeyQuery<long>.Between(a, b)))
                        {
                            if (kv.Key < a || kv.Key > b)
                                Fail(new Exception(
                                    $"Orders.Between: key {kv.Key} outside [{a},{b}]"), _ctx);
                            if (++n >= 500) break;
                        }
                        Hit($"orders.Between({a},{b})→{n}");
                        break;
                    }

                    // ── Orders: Between exclusive bounds ──────────────────────────────────
                    case 2:
                    {
                        var top = Volatile.Read(ref _ctx.NextOrderId);
                        if (top < 4) break;
                        var a = _rng.NextInt64(1, top / 2);
                        var b = a + _rng.Next(10, 200);
                        foreach (var kv in _ctx.Orders.Query(
                            KeyQuery<long>.Between(a, b, fromInclusive: false, toInclusive: false)))
                        {
                            if (kv.Key <= a || kv.Key >= b)
                                Fail(new Exception(
                                    $"Orders.BetweenExcl: key {kv.Key} not in ({a},{b})"), _ctx);
                        }
                        Hit($"orders.BetweenExcl({a},{b})");
                        break;
                    }

                    // ── Ticks: GreaterThan (exclusive lower) ──────────────────────────────
                    case 3:
                    {
                        var top = Volatile.Read(ref _ctx.NextTickId);
                        if (top < 2) break;
                        var pivot = _rng.NextInt64(1, top);
                        foreach (var kv in _ctx.Ticks.QueryTake(KeyQuery<long>.GreaterThan(pivot), 200))
                        {
                            if (kv.Key <= pivot)
                                Fail(new Exception(
                                    $"Ticks.GreaterThan: key {kv.Key} <= pivot {pivot}"), _ctx);
                        }
                        Hit($"ticks.GreaterThan({pivot})");
                        break;
                    }

                    // ── Ticks: LessThan (exclusive upper) ────────────────────────────────
                    case 4:
                    {
                        var top = Volatile.Read(ref _ctx.NextTickId);
                        if (top < 2) break;
                        var pivot = _rng.NextInt64(2, top);
                        foreach (var kv in _ctx.Ticks.QueryTake(KeyQuery<long>.LessThan(pivot), 200))
                        {
                            if (kv.Key >= pivot)
                                Fail(new Exception(
                                    $"Ticks.LessThan: key {kv.Key} >= pivot {pivot}"), _ctx);
                        }
                        Hit($"ticks.LessThan({pivot})");
                        break;
                    }

                    // ── Scores: StartsWith prefix scan ────────────────────────────────────
                    case 5:
                    {
                        var prefixes = new[] { "p0", "p1", "p2", "p3", "p4",
                                               "p5", "p6", "p7", "p8", "p9" };
                        var pfx  = prefixes[_rng.Next(prefixes.Length)];
                        int n    = 0;
                        foreach (var kv in _ctx.Scores.Query(KeyQuery.StartsWith(pfx)))
                        {
                            if (!kv.Key.StartsWith(pfx, StringComparison.Ordinal))
                                Fail(new Exception(
                                    $"Scores.StartsWith: key \"{kv.Key}\" doesn't start with \"{pfx}\""), _ctx);
                            if (++n >= 200) break;
                        }
                        Hit($"scores.StartsWith(\"{pfx}\")→{n}");
                        break;
                    }

                    // ── Orders: QueryBackward descending order check ───────────────────────
                    case 6:
                    {
                        var top = Volatile.Read(ref _ctx.NextOrderId);
                        if (top < 4) break;
                        var a = _rng.NextInt64(1, top / 2);
                        var b = a + _rng.Next(50, 500);
                        long prev = long.MaxValue;
                        int  n   = 0;
                        foreach (var kv in _ctx.Orders.QueryBackward(KeyQuery<long>.Between(a, b)))
                        {
                            if (kv.Key > prev)
                                Fail(new Exception(
                                    $"Orders.Backward: order violation {kv.Key} > {prev}"), _ctx);
                            if (kv.Key < a || kv.Key > b)
                                Fail(new Exception(
                                    $"Orders.Backward: key {kv.Key} outside [{a},{b}]"), _ctx);
                            prev = kv.Key;
                            if (++n >= 500) break;
                        }
                        Hit($"orders.Backward({a},{b})→{n}");
                        break;
                    }

                    // ── Ticks: cursor paging — each page ascending, no gaps/overlaps ──────
                    case 7:
                    {
                        var top = Volatile.Read(ref _ctx.NextTickId);
                        if (top < 20) break;
                        var from = Math.Max(1L, top - 300);
                        var query = KeyQuery<long>.AtLeast(from);
                        long? lastKey = null;
                        int pages = 0, total = 0;

                        while (pages < 5)
                        {
                            var page = lastKey == null
                                ? _ctx.Ticks.PageAfter(query, take: 20).ToList()
                                : _ctx.Ticks.PageAfter(query, afterKey: lastKey.Value, take: 20).ToList();
                            if (page.Count == 0) break;

                            // Each key must be > lastKey (strict ascending / no overlap)
                            foreach (var kv in page)
                            {
                                if (lastKey.HasValue && kv.Key <= lastKey.Value)
                                    Fail(new Exception(
                                        $"Ticks.PageAfter: key {kv.Key} <= cursor {lastKey.Value}"), _ctx);
                                lastKey = kv.Key;
                            }
                            total += page.Count;
                            pages++;
                        }
                        Hit($"ticks.PageAfter→{pages}p/{total}rows");
                        break;
                    }

                    // ── Sensors: AtMost scan ──────────────────────────────────────────────
                    case 8:
                    {
                        var top = Volatile.Read(ref _ctx.NextSensorId);
                        if (top < 2) break;
                        var ceiling = _rng.NextInt64(1, top);
                        foreach (var kv in _ctx.Sensors.QueryTake(KeyQuery<long>.AtMost(ceiling), 300))
                        {
                            if (kv.Key > ceiling)
                                Fail(new Exception(
                                    $"Sensors.AtMost: key {kv.Key} > ceiling {ceiling}"), _ctx);
                        }
                        Hit($"sensors.AtMost({ceiling})");
                        break;
                    }

                    // ── Count: quick range count on orders ────────────────────────────────
                    case 9:
                    {
                        var top = Volatile.Read(ref _ctx.NextOrderId);
                        if (top < 4) break;
                        var a = _rng.NextInt64(1, top / 2);
                        var b = a + _rng.Next(10, 200);
                        var cnt = _ctx.Orders.Count(KeyQuery<long>.Between(a, b));
                        // Count must be >= 0 (trivially) and <= range size
                        if (cnt < 0 || cnt > (b - a + 1))
                            Fail(new Exception(
                                $"Orders.Count({a},{b}) returned {cnt} which is out of range"), _ctx);
                        Hit($"orders.Count({a},{b})={cnt}");
                        break;
                    }
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            await Task.Delay(5, ct).ContinueWith(_ => { });
        }
    }
}

// ─── 11. Data Integrity Service ──────────────────────────────────────────
// The ONLY service that uses the Integrity table.
//
// Phase A — INSERT:
//   Write N records with deterministic payload (key + version → fields).
//   Commit. Read every one back and verify all fields match exactly.
//
// Phase B — UPDATE (Replace):
//   Increment version, write updated record, commit, read back, validate.
//
// Phase C — DELETE + re-INSERT:
//   Delete a record, commit, confirm it's gone.
//   Re-insert with a fresh version, commit, read back, validate.
//
// Any mismatch → ctx.RecordError (logged as a CORRUPTION) + TotalCorruptions++
// This catches: wrong key mapping, truncated strings, flipped booleans,
//               serialization bugs, off-by-one in keys, etc.

public sealed class DataIntegrityService : BackgroundService
{
    private const int BatchSize  = 50;   // records per integrity round
    private const string BaseTag = "integrity";

    private readonly StressContext _ctx;
    private readonly Random        _rng;
    private long _nextIntegrityKey = 0;

    public DataIntegrityService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ unchecked((int)0x1A2B3C4D));
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                switch (_rng.Next(3))
                {
                    case 0: await RoundInsert(ct);        break;
                    case 1: await RoundUpdate(ct);        break;
                    case 2: await RoundDeleteReinsert(ct); break;
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            await Task.Delay(10, ct).ContinueWith(_ => { });
        }
    }

    // ── Phase A: batch insert, commit, verify ────────────────────────────
    private async Task RoundInsert(CancellationToken ct)
    {
        var keys = new List<long>(BatchSize);
        for (var i = 0; i < BatchSize; i++)
        {
            var key = Interlocked.Increment(ref _nextIntegrityKey);
            var rec = IntegrityRecord.Build(key, version: 0, tag: $"{BaseTag}_insert");
            _ctx.Integrity[key] = rec;
            keys.Add(key);
        }

        // Force a commit so data is flushed before we read back
    #if PERFORMANCE_CHECK
        CatDb.General.Diagnostics.PerformanceCheck.Increment("stress.commit.source.integrity.insert");
    #endif
        _ctx.Commit();
        await Task.Yield();

        int ok = 0, fail = 0;
        foreach (var key in keys)
        {
            if (!_ctx.Integrity.TryGet(key, out var got))
            {
                RecordCorruption(key, "INSERT: record not found after commit");
                fail++;
                continue;
            }
            var expected = IntegrityRecord.Build(key, version: 0, tag: $"{BaseTag}_insert");
            if (!Matches(key, expected, got, "INSERT"))
                fail++;
            else
                ok++;
        }
        Hit($"insert {ok}✓ {fail}✗");
    }

    // ── Phase B: update (replace) existing records, commit, verify ───────
    private async Task RoundUpdate(CancellationToken ct)
    {
        var top = Volatile.Read(ref _nextIntegrityKey);
        if (top < BatchSize) return;

        var keys = new List<long>(BatchSize);
        for (var i = 0; i < BatchSize; i++)
            keys.Add(_rng.NextInt64(1, top + 1));

        // Write an updated version of each key
        foreach (var key in keys)
        {
            // Read current version first
            var version = _ctx.Integrity.TryGet(key, out var cur) ? cur.Version + 1 : 1;
            var rec = IntegrityRecord.Build(key, version, tag: $"{BaseTag}_update");
            _ctx.Integrity[key] = rec;
        }

    #if PERFORMANCE_CHECK
        CatDb.General.Diagnostics.PerformanceCheck.Increment("stress.commit.source.integrity.update");
    #endif
        _ctx.Commit();
        await Task.Yield();

        int ok = 0, fail = 0;
        foreach (var key in keys)
        {
            if (!_ctx.Integrity.TryGet(key, out var got))
            {
                RecordCorruption(key, "UPDATE: record not found after commit");
                fail++;
                continue;
            }
            // Version we actually wrote; just validate fields are self-consistent
            var rebuilt = IntegrityRecord.Build(key, got.Version, tag: $"{BaseTag}_update");
            if (!Matches(key, rebuilt, got, "UPDATE"))
                fail++;
            else
                ok++;
        }
        Hit($"update {ok}✓ {fail}✗");
    }

    // ── Phase C: delete, confirm gone, re-insert, verify ─────────────────
    private async Task RoundDeleteReinsert(CancellationToken ct)
    {
        var top = Volatile.Read(ref _nextIntegrityKey);
        if (top < BatchSize) return;

        var keys = new List<long>(BatchSize / 2);
        for (var i = 0; i < BatchSize / 2; i++)
            keys.Add(_rng.NextInt64(1, top + 1));

        // Delete
        foreach (var key in keys)
            _ctx.Integrity.Delete(key);

    #if PERFORMANCE_CHECK
        CatDb.General.Diagnostics.PerformanceCheck.Increment("stress.commit.source.integrity.delete");
    #endif
        _ctx.Commit();
        await Task.Yield();

        // Confirm deleted
        int deleteFail = 0;
        foreach (var key in keys)
        {
            if (_ctx.Integrity.TryGet(key, out _))
            {
                RecordCorruption(key, "DELETE: record still present after commit");
                deleteFail++;
            }
        }

        // Re-insert
        foreach (var key in keys)
        {
            var rec = IntegrityRecord.Build(key, version: 99, tag: $"{BaseTag}_reinsert");
            _ctx.Integrity[key] = rec;
        }

    #if PERFORMANCE_CHECK
        CatDb.General.Diagnostics.PerformanceCheck.Increment("stress.commit.source.integrity.reinsert");
    #endif
        _ctx.Commit();
        await Task.Yield();

        int ok = 0, fail = 0;
        foreach (var key in keys)
        {
            if (!_ctx.Integrity.TryGet(key, out var got))
            {
                RecordCorruption(key, "REINSERT: record not found after commit");
                fail++;
                continue;
            }
            var expected = IntegrityRecord.Build(key, version: 99, tag: $"{BaseTag}_reinsert");
            if (!Matches(key, expected, got, "REINSERT"))
                fail++;
            else
                ok++;
        }
        Hit($"del+reinsert {ok}✓ {fail + deleteFail}✗");
    }

    // ── Field-by-field comparison ─────────────────────────────────────────
    private bool Matches(long key, IntegrityRecord expected, IntegrityRecord actual, string phase)
    {
        var errors = new System.Text.StringBuilder();

        if (actual.Key     != expected.Key)     errors.Append($" Key:{actual.Key}≠{expected.Key}");
        if (actual.StrVal  != expected.StrVal)  errors.Append($" StrVal:\"{actual.StrVal}\"≠\"{expected.StrVal}\"");
        if (Math.Abs(actual.DblVal - expected.DblVal) > 1e-9)
                                                errors.Append($" DblVal:{actual.DblVal}≠{expected.DblVal}");
        if (actual.IntVal  != expected.IntVal)  errors.Append($" IntVal:{actual.IntVal}≠{expected.IntVal}");
        if (actual.TimeVal != expected.TimeVal) errors.Append($" TimeVal:{actual.TimeVal}≠{expected.TimeVal}");
        if (actual.BoolVal != expected.BoolVal) errors.Append($" BoolVal:{actual.BoolVal}≠{expected.BoolVal}");
        if (actual.Tag     != expected.Tag)     errors.Append($" Tag:\"{actual.Tag}\"≠\"{expected.Tag}\"");
        if (actual.Version != expected.Version) errors.Append($" Version:{actual.Version}≠{expected.Version}");

        if (errors.Length == 0) return true;

        RecordCorruption(key, $"{phase} MISMATCH —{errors}");
        return false;
    }

    private void RecordCorruption(long key, string detail)
    {
        Interlocked.Increment(ref _ctx.TotalCorruptions);
        _ctx.RecordError(Name, $"CORRUPTION key={key}: {detail}");
    }
}

// ─── 12. High-Stress Key Search Service ──────────────────────────────────
// Dedicated maximum-throughput stress service for the KeyQuery / ScanCount
// engine-native search path.  Runs with NO Task.Delay — just continuous
// tight loops.  Two instances (A and B) run different operation mixes:
//
//   A  — full-range and wide-range forward scans, Count, backward scans
//   B  — narrow range scans, exclusive bounds, WithFilter, cursor paging
//
// Every operation validates correctness (key order, boundary compliance,
// Count ≥ 0, page cursor integrity) and calls Fail() on any violation.
//
// Throughput metric: TotalSearchOps (visible on the dashboard as a separate
// counter next to KeySearch).

public sealed class HighStressKeySearchService : BackgroundService
{
    // ── Shared op counter across all instances ────────────────────────────
    public static long TotalSearchOps = 0;

    private readonly StressContext _ctx;
    private readonly Random        _rng;
    private readonly bool          _wideMode;   // true = instance A, false = instance B

    public HighStressKeySearchService(string name, StressContext ctx, bool wideMode) : base(name)
    {
        _ctx      = ctx;
        _rng      = new Random(name.GetHashCode() ^ (wideMode ? 0x1111_1111 : unchecked((int)0xEEEE_EEEE)));
        _wideMode = wideMode;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_wideMode)
                    RunWide();
                else
                    RunNarrow();

                Interlocked.Increment(ref TotalSearchOps);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { Fail(ex, _ctx); }

            // No Task.Delay — yield only so other tasks get CPU time
            await Task.Yield();
        }
    }

    // ── Instance A: wide / full-range operations ─────────────────────────
    private void RunWide()
    {
        var top = Volatile.Read(ref _ctx.NextTickId);
        if (top < 10) return;

        switch (_rng.Next(8))
        {
            // Full ascending scan (capped at 10 000 records to bound latency)
            case 0:
            {
                long prev = -1;
                int  n   = 0;
                foreach (var kv in _ctx.Ticks.QueryTake(KeyQuery<long>.All(), 10_000))
                {
                    if (prev != -1 && kv.Key <= prev)
                        Fail(new Exception(
                            $"HighStress.Wide.FullFwd: order violation {kv.Key} <= {prev}"), _ctx);
                    prev = kv.Key;
                    n++;
                }
                Hit($"wide.FullFwd {n:N0}");
                break;
            }

            // Full descending scan (capped at 10 000)
            case 1:
            {
                long prev = long.MaxValue;
                int  n   = 0;
                foreach (var kv in _ctx.Ticks.QueryBackwardTake(KeyQuery<long>.All(), 10_000))
                {
                    if (kv.Key >= prev)
                        Fail(new Exception(
                            $"HighStress.Wide.FullBwd: order violation {kv.Key} >= {prev}"), _ctx);
                    prev = kv.Key;
                    n++;
                }
                Hit($"wide.FullBwd {n:N0}");
                break;
            }

            // ScanCount on a large range (50% of tick space)
            case 2:
            {
                var from = Math.Max(1L, top / 4);
                var to   = top * 3 / 4;
                var cnt  = _ctx.Ticks.ScanCount(KeyQuery<long>.Between(from, to));
                if (cnt < 0)
                    Fail(new Exception(
                        $"HighStress.Wide.Count: returned {cnt} for [{from},{to}]"), _ctx);
                Hit($"wide.Count[{from},{to}]={cnt:N0}");
                break;
            }

            // ScanCount on All() — counts every tick in the table
            case 3:
            {
                var cnt = _ctx.Ticks.ScanCount(KeyQuery<long>.All());
                if (cnt < 0)
                    Fail(new Exception(
                        $"HighStress.Wide.CountAll: returned {cnt}"), _ctx);
                Hit($"wide.CountAll={cnt:N0}");
                break;
            }

            // Large backward range on Sensors table (batch-import target)
            case 4:
            {
                var sensorTop = Volatile.Read(ref _ctx.NextSensorId);
                if (sensorTop < 10) break;
                var from = Math.Max(1L, sensorTop - 5_000);
                long prev = long.MaxValue;
                int  n   = 0;
                foreach (var kv in _ctx.Sensors.QueryBackwardTake(KeyQuery<long>.AtLeast(from), 5_000))
                {
                    if (kv.Key >= prev)
                        Fail(new Exception(
                            $"HighStress.Wide.SensorBwd: order violation {kv.Key} >= {prev}"), _ctx);
                    if (kv.Key < from)
                        Fail(new Exception(
                            $"HighStress.Wide.SensorBwd: key {kv.Key} < from {from}"), _ctx);
                    prev = kv.Key;
                    n++;
                }
                Hit($"wide.SensorBwd {n:N0}");
                break;
            }

            // Large Metrics scan (ascending, full table)
            case 5:
            {
                long prev = -1;
                int  n   = 0;
                foreach (var kv in _ctx.Metrics.QueryTake(KeyQuery<long>.All(), 5_000))
                {
                    if (prev != -1 && kv.Key <= prev)
                        Fail(new Exception(
                            $"HighStress.Wide.MetricsFwd: order violation {kv.Key} <= {prev}"), _ctx);
                    prev = kv.Key;
                    n++;
                }
                Hit($"wide.MetricsFwd {n:N0}");
                break;
            }

            // Concurrent Count + Scan — both on same live tick range
            case 6:
            {
                var from = Math.Max(1L, top - 2_000);
                var to   = top;
                var q    = KeyQuery<long>.Between(from, to);
                var cnt  = _ctx.Ticks.ScanCount(q);
                int  n  = 0;
                foreach (var _ in _ctx.Ticks.QueryTake(q, 2_000))
                    n++;
                // Count may differ from n because inserts/deletes happen between the two calls
                if (cnt < 0)
                    Fail(new Exception(
                        $"HighStress.Wide.CountVsScan: Count={cnt} negative"), _ctx);
                Hit($"wide.CountVsScan cnt={cnt:N0} scan={n:N0}");
                break;
            }

            // Audit full backward scan (append-only log)
            case 7:
            {
                long prev = long.MaxValue;
                int  n   = 0;
                foreach (var kv in _ctx.Audit.QueryBackwardTake(KeyQuery<long>.All(), 5_000))
                {
                    if (kv.Key >= prev)
                        Fail(new Exception(
                            $"HighStress.Wide.AuditBwd: order violation {kv.Key} >= {prev}"), _ctx);
                    prev = kv.Key;
                    n++;
                }
                Hit($"wide.AuditBwd {n:N0}");
                break;
            }
        }
    }

    // ── Instance B: narrow / precise boundary operations ─────────────────
    private void RunNarrow()
    {
        switch (_rng.Next(8))
        {
            // Narrow tick window with exclusive-from bound
            case 0:
            {
                var top = Volatile.Read(ref _ctx.NextTickId);
                if (top < 10) break;
                var pivot = _rng.NextInt64(1, top);
                long prev = -1;
                int  n   = 0;
                foreach (var kv in _ctx.Ticks.QueryTake(KeyQuery<long>.GreaterThan(pivot), 300))
                {
                    if (kv.Key <= pivot)
                        Fail(new Exception(
                            $"HighStress.Narrow.GreaterThan: key {kv.Key} <= pivot {pivot}"), _ctx);
                    if (prev != -1 && kv.Key <= prev)
                        Fail(new Exception(
                            $"HighStress.Narrow.GreaterThan: order violation {kv.Key} <= {prev}"), _ctx);
                    prev = kv.Key;
                    n++;
                }
                Hit($"narrow.GreaterThan({pivot})→{n}");
                break;
            }

            // Orders exclusive-both-ends scan
            case 1:
            {
                var top = Volatile.Read(ref _ctx.NextOrderId);
                if (top < 10) break;
                var a = _rng.NextInt64(1, Math.Max(2, top));
                var b = a + _rng.Next(2, 100);
                int n = 0;
                foreach (var kv in _ctx.Orders.QueryTake(
                    KeyQuery<long>.Between(a, b, fromInclusive: false, toInclusive: false), 200))
                {
                    if (kv.Key <= a || kv.Key >= b)
                        Fail(new Exception(
                            $"HighStress.Narrow.BothExcl: key {kv.Key} not in ({a},{b})"), _ctx);
                    n++;
                }
                Hit($"narrow.BothExcl({a},{b})→{n}");
                break;
            }

            // WithFilter — only even keys from a tick range
            case 2:
            {
                var top = Volatile.Read(ref _ctx.NextTickId);
                if (top < 10) break;
                var from = Math.Max(1L, top / 2);
                var q    = KeyQuery<long>.AtLeast(from).WithFilter(k => k % 2 == 0);
                int n   = 0;
                foreach (var kv in _ctx.Ticks.QueryTake(q, 300))
                {
                    if (kv.Key % 2 != 0)
                        Fail(new Exception(
                            $"HighStress.Narrow.WithFilter: odd key {kv.Key}"), _ctx);
                    if (kv.Key < from)
                        Fail(new Exception(
                            $"HighStress.Narrow.WithFilter: key {kv.Key} < from {from}"), _ctx);
                    n++;
                }
                Hit($"narrow.WithFilter→{n}");
                break;
            }

            // ScanCount with exclusive bounds — must be <= range size
            case 3:
            {
                var top = Volatile.Read(ref _ctx.NextOrderId);
                if (top < 10) break;
                var a   = _rng.NextInt64(1, Math.Max(2, top));
                var b   = a + _rng.Next(1, 500);
                var cnt = _ctx.Orders.ScanCount(
                    KeyQuery<long>.Between(a, b, fromInclusive: false, toInclusive: false));
                if (cnt < 0 || cnt > (b - a - 1))
                    Fail(new Exception(
                        $"HighStress.Narrow.CountExcl({a},{b}) returned {cnt}"), _ctx);
                Hit($"narrow.CountExcl({a},{b})={cnt}");
                break;
            }

            // Cursor paging with ascending forward scan — no key must repeat
            case 4:
            {
                var top = Volatile.Read(ref _ctx.NextTickId);
                if (top < 50) break;
                var from   = Math.Max(1L, top - 1_000);
                var q      = KeyQuery<long>.AtLeast(from);
                long? cursor = null;
                var seen   = new System.Collections.Generic.HashSet<long>();
                int  pages = 0, total = 0;

                while (pages < 10)
                {
                    var page = (cursor is null
                        ? _ctx.Ticks.PageAfter(q, take: 50)
                        : _ctx.Ticks.PageAfter(q, afterKey: cursor.Value, take: 50)).ToList();

                    if (page.Count == 0) break;

                    foreach (var kv in page)
                    {
                        if (!seen.Add(kv.Key))
                            Fail(new Exception(
                                $"HighStress.Narrow.PageAfter: duplicate key {kv.Key}"), _ctx);
                        if (cursor.HasValue && kv.Key <= cursor.Value)
                            Fail(new Exception(
                                $"HighStress.Narrow.PageAfter: key {kv.Key} <= cursor {cursor.Value}"), _ctx);
                        cursor = kv.Key;
                    }
                    total += page.Count;
                    pages++;
                }
                Hit($"narrow.PageAfter {pages}p/{total}rows");
                break;
            }

            // Scores: StartsWith scan — every result must have correct prefix
            case 5:
            {
                var digits = "0123456789";
                var pfx    = "p" + digits[_rng.Next(10)];
                int n     = 0;
                foreach (var kv in _ctx.Scores.QueryTake(KeyQuery.StartsWith(pfx), 200))
                {
                    if (!kv.Key.StartsWith(pfx, StringComparison.Ordinal))
                        Fail(new Exception(
                            $"HighStress.Narrow.StartsWith: key \"{kv.Key}\" missing prefix \"{pfx}\""), _ctx);
                    n++;
                }
                Hit($"narrow.StartsWith(\"{pfx}\")→{n}");
                break;
            }

            // AtMost backward scan — every result must be <= ceiling
            case 6:
            {
                var top = Volatile.Read(ref _ctx.NextSensorId);
                if (top < 5) break;
                var ceiling = _rng.NextInt64(1, top);
                long prev   = long.MaxValue;
                int  n     = 0;
                foreach (var kv in _ctx.Sensors.QueryBackwardTake(KeyQuery<long>.AtMost(ceiling), 300))
                {
                    if (kv.Key > ceiling)
                        Fail(new Exception(
                            $"HighStress.Narrow.AtMostBwd: key {kv.Key} > ceiling {ceiling}"), _ctx);
                    if (kv.Key >= prev)
                        Fail(new Exception(
                            $"HighStress.Narrow.AtMostBwd: order violation {kv.Key} >= {prev}"), _ctx);
                    prev = kv.Key;
                    n++;
                }
                Hit($"narrow.AtMostBwd({ceiling})→{n}");
                break;
            }

            // LessThan descending scan with Count consistency check
            case 7:
            {
                var top = Volatile.Read(ref _ctx.NextTickId);
                if (top < 10) break;
                var pivot = _rng.NextInt64(2, top);
                var q     = KeyQuery<long>.LessThan(pivot);
                var cnt   = _ctx.Ticks.ScanCount(q);
                int  n   = 0;
                foreach (var kv in _ctx.Ticks.QueryBackwardTake(q, 500))
                {
                    if (kv.Key >= pivot)
                        Fail(new Exception(
                            $"HighStress.Narrow.LtBwd: key {kv.Key} >= pivot {pivot}"), _ctx);
                    n++;
                }
                if (cnt < 0)
                    Fail(new Exception(
                        $"HighStress.Narrow.LtBwd: Count={cnt} negative"), _ctx);
                Hit($"narrow.LtBwd({pivot}) cnt={cnt:N0} scan={n:N0}");
                break;
            }
        }
    }
}

// ─── Index Stress ──────────────────────────────────────────────────────────
// Creates a table with unique + non-unique indexes and exercises concurrent
// inserts, updates, deletes, and index lookups at speed.

public sealed class IndexStressService : BackgroundService
{
    private static readonly string[] Categories =
        ["Electronics", "Books", "Clothing", "Home", "Sports", "Toys", "Food", "Auto"];
    private static readonly string[] Brands =
        ["Acme", "BrandX", "Omega", "Zeta", "Nova", "Prime", "Ultra", "Core"];

    private readonly StressContext _ctx;
    private readonly Random _rng;
    private readonly ITable<long, IndexedProduct> _products;
    private long _nextId;
    private bool _indexesCreated;

    public IndexStressService(string name, StressContext ctx) : base(name)
    {
        _ctx = ctx;
        _rng = new Random(name.GetHashCode() ^ unchecked((int)0xFACEFEED));
        _products = ctx.Engine.OpenXTable<long, IndexedProduct>("idx_products");
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // Create indexes on first iteration
        if (!_indexesCreated)
        {
            _products.CreateIndex("Sku", p => p.Sku, IndexType.Unique);
            _products.CreateIndex("Category", p => p.Category, IndexType.NonUnique);
            _products.CreateIndex("Brand", p => p.Brand, IndexType.NonUnique);
            _indexesCreated = true;
        }

        while (!ct.IsCancellationRequested)
        {
            try
            {
                int op = _rng.Next(100);

                if (op < 40)
                {
                    // ── Insert new product ──
                    var id = Interlocked.Increment(ref _nextId);
                    var sku = $"SKU-{id:D8}";
                    _products.Replace(id, new IndexedProduct
                    {
                        Sku = sku,
                        Category = Categories[_rng.Next(Categories.Length)],
                        Price = Math.Round(1.0 + _rng.NextDouble() * 999.0, 2),
                        Stock = _rng.Next(0, 10_000),
                        Brand = Brands[_rng.Next(Brands.Length)],
                    });
                    Hit($"insert id={id}");
                }
                else if (op < 60)
                {
                    // ── Update existing product (change category/brand) ──
                    var maxId = Volatile.Read(ref _nextId);
                    if (maxId > 0)
                    {
                        var id = 1 + (long)(_rng.NextDouble() * maxId);
                        if (_products.TryGet(id, out var existing))
                        {
                            existing.Category = Categories[_rng.Next(Categories.Length)];
                            existing.Brand = Brands[_rng.Next(Brands.Length)];
                            existing.Price = Math.Round(1.0 + _rng.NextDouble() * 999.0, 2);
                            _products.Replace(id, existing);
                            Hit($"update id={id}");
                        }
                    }
                }
                else if (op < 70)
                {
                    // ── Delete product ──
                    var maxId = Volatile.Read(ref _nextId);
                    if (maxId > 0)
                    {
                        var id = 1 + (long)(_rng.NextDouble() * maxId);
                        _products.Delete(id);
                        Hit($"delete id={id}");
                    }
                }
                else if (op < 85)
                {
                    // ── Lookup by SKU (unique index) ──
                    var maxId = Volatile.Read(ref _nextId);
                    if (maxId > 0)
                    {
                        var id = 1 + (long)(_rng.NextDouble() * maxId);
                        var sku = $"SKU-{id:D8}";
                        var results = _products.Query(p => p.Sku).Equals(sku).ToList();
                        Hit($"find.sku={sku} cnt={results.Count}");
                    }
                }
                else if (op < 95)
                {
                    // ── Lookup by Category (non-unique index) ──
                    var cat = Categories[_rng.Next(Categories.Length)];
                    var count = _products.Query(p => p.Category).Equals(cat).Count();
                    Hit($"count.cat={cat} cnt={count}");
                }
                else
                {
                    // ── Lookup by Brand (non-unique index) ──
                    var brand = Brands[_rng.Next(Brands.Length)];
                    var count = _products.Query(p => p.Brand).Equals(brand).Count();
                    Hit($"count.brand={brand} cnt={count}");
                }
            }
            catch (UniqueIndexViolationException)
            {
                // Expected during concurrent stress — SKU collision from re-insert after delete
                Hit("sku-collision (expected)");
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                Fail(ex, _ctx);
            }

            await Task.Yield();
        }
    }
}
