namespace CatDb.Server.Models;

public sealed class UpsertAdminUserRequest
{
    public string UserName { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string GlobalPermissions { get; set; } = GlobalPermission.None.ToString();
    public Dictionary<string, string> DatabasePermissions { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
