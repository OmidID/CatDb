namespace CatDb.Server;

/// <summary>Shared runtime state exposed to the health check and any future endpoints.</summary>
public sealed class ServerState
{
    public bool   IsRunning { get; set; }
    public int    Port      { get; set; }
    public string DatabaseDirectory { get; set; } = string.Empty;
    public string DefaultDatabaseName { get; set; } = string.Empty;
}
