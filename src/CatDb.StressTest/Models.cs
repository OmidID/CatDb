namespace CatDb.StressTest;

// ─── Market / financial ────────────────────────────────────────────────────

public class Tick
{
    public string Symbol    { get; set; } = "";
    public DateTime Timestamp { get; set; }
    public double Bid       { get; set; }
    public double Ask       { get; set; }
    public int    BidSize   { get; set; }
    public int    AskSize   { get; set; }
    public string Provider  { get; set; } = "";
}

public class Order
{
    public string Symbol         { get; set; } = "";
    public string Side           { get; set; } = "";   // BUY / SELL
    public double Price          { get; set; }
    public double Quantity       { get; set; }
    public double FilledQuantity { get; set; }
    public string Status         { get; set; } = "";   // OPEN / PARTIAL / FILLED / CANCELLED
    public DateTime CreatedAt    { get; set; }
    public DateTime UpdatedAt    { get; set; }
}

public class MetricSnapshot
{
    public string   Name        { get; set; } = "";
    public double   Value       { get; set; }
    public double   Min         { get; set; }
    public double   Max         { get; set; }
    public double   Avg         { get; set; }
    public int      SampleCount { get; set; }
    public DateTime Timestamp   { get; set; }
}

// ─── Web / session ─────────────────────────────────────────────────────────

public class UserSession
{
    public string   UserId       { get; set; } = "";
    public string   IpAddress    { get; set; } = "";
    public DateTime CreatedAt    { get; set; }
    public DateTime LastActivity { get; set; }
    public int      RequestCount { get; set; }
    public bool     IsActive     { get; set; }
    public string   Role         { get; set; } = "";
}

// ─── Gaming ────────────────────────────────────────────────────────────────

public class PlayerScore
{
    public string   Username    { get; set; } = "";
    public long     Score       { get; set; }
    public int      Level       { get; set; }
    public DateTime LastUpdated { get; set; }
}

// ─── IoT / sensors ─────────────────────────────────────────────────────────

public class SensorReading
{
    public string   SensorId    { get; set; } = "";
    public double   Temperature { get; set; }
    public double   Humidity    { get; set; }
    public double   Pressure    { get; set; }
    public double   Voltage     { get; set; }
    public DateTime ReadingTime { get; set; }
}

// ─── Audit ─────────────────────────────────────────────────────────────────

public class AuditEntry
{
    public string   ServiceName { get; set; } = "";
    public string   Operation   { get; set; } = "";
    public string   Detail      { get; set; } = "";
    public DateTime Timestamp   { get; set; }
    public bool     Success     { get; set; }
}
