using CatDb.AdminUi.Clients;
using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;
using CatDb.AdminUi.Resources;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Localization;

namespace CatDb.AdminUi.Components.Layout;

public partial class DatabaseTreeMenu : ComponentBase
{
    private const int MaxDatabasesInNav = 200;

    [Inject] private IAdminDatabasesClient DatabasesClient { get; set; } = default!;
    [Inject] private ITablesClient TablesClient { get; set; } = default!;
    [Inject] private IStringLocalizer<SharedResources> Localizer { get; set; } = default!;

    private bool _rootExpanded;
    private bool _rootLoaded;
    private bool _rootLoading;
    private List<DatabaseRecord> _databases = [];

    private readonly HashSet<string> _expandedDbs = [];
    private readonly HashSet<string> _dbLoading = [];
    private readonly Dictionary<string, List<TableSummary>> _dbTables = [];

    private static string ChevronClass(bool expanded) =>
        expanded ? "h-4 w-4 rotate-90 transition-transform" : "h-4 w-4 transition-transform";

    private async Task ToggleRootAsync()
    {
        _rootExpanded = !_rootExpanded;
        if (_rootExpanded && !_rootLoaded)
            await LoadDatabasesAsync();
    }

    private async Task RefreshRootAsync()
    {
        _rootExpanded = true;
        await LoadDatabasesAsync();
    }

    private async Task LoadDatabasesAsync()
    {
        _rootLoading = true;
        StateHasChanged();
        try
        {
            var result = await DatabasesClient.ListAsync(1, MaxDatabasesInNav);
            _databases = result.Items;
            _rootLoaded = true;
        }
        catch (ApiException)
        {
            // Nav tree stays collapsed/empty on failure; the page the user navigates to
            // (Databases) surfaces the real error banner.
        }
        finally
        {
            _rootLoading = false;
        }
    }

    private async Task ToggleDatabaseAsync(string database)
    {
        if (!_expandedDbs.Add(database))
        {
            _expandedDbs.Remove(database);
            return;
        }

        if (!_dbTables.ContainsKey(database))
            await LoadTablesAsync(database);
    }

    private async Task RefreshDatabaseAsync(string database)
    {
        _expandedDbs.Add(database);
        await LoadTablesAsync(database);
    }

    private async Task LoadTablesAsync(string database)
    {
        _dbLoading.Add(database);
        StateHasChanged();
        try
        {
            _dbTables[database] = await TablesClient.ListAsync(database);
        }
        catch (ApiException)
        {
            _dbTables[database] = [];
        }
        finally
        {
            _dbLoading.Remove(database);
        }
    }
}
