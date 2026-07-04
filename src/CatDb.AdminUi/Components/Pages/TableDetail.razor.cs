using System.Text.Json;
using CatDb.AdminUi.Clients;
using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;
using Microsoft.AspNetCore.Components;

namespace CatDb.AdminUi.Components.Pages;

public partial class TableDetail : ComponentBase
{
    [Parameter] public string DatabaseName { get; set; } = "";
    [Parameter] public string TableName { get; set; } = "";

    [Inject] private ITablesClient TablesClient { get; set; } = default!;

    private TableInfo? _table;
    private List<IndexInfo> _indexes = [];
    private string? _errorMessage;
    private bool _isLoading = true;
    private bool _showCreateIndex;
    private bool _isCreatingIndex;
    private string _newIndexName = "";
    private string _newIndexMembers = "";
    private string _newIndexType = IndexTypes.NonUnique;

    protected override Task OnInitializedAsync() => LoadAsync();

    private async Task LoadAsync()
    {
        _isLoading = true;
        try
        {
            _table = await TablesClient.GetAsync(DatabaseName, TableName);
            _indexes = await TablesClient.ListIndexesAsync(DatabaseName, TableName);
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
        finally
        {
            _isLoading = false;
        }
    }

    private static string Pretty(JsonElement element) =>
        JsonSerializer.Serialize(element, new JsonSerializerOptions { WriteIndented = true });

    private async Task CreateIndexAsync()
    {
        if (string.IsNullOrWhiteSpace(_newIndexName) || string.IsNullOrWhiteSpace(_newIndexMembers))
        {
            _errorMessage = "Index name and at least one member field are required.";
            return;
        }

        _isCreatingIndex = true;
        try
        {
            var members = _newIndexMembers.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).ToList();
            await TablesClient.CreateIndexAsync(DatabaseName, TableName, new CreateIndexRequest(_newIndexName.Trim(), members, _newIndexType));
            _newIndexName = "";
            _newIndexMembers = "";
            _showCreateIndex = false;
            await LoadAsync();
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
        finally
        {
            _isCreatingIndex = false;
        }
    }

    private async Task DeleteIndexAsync(string indexName)
    {
        try
        {
            await TablesClient.DeleteIndexAsync(DatabaseName, TableName, indexName);
            await LoadAsync();
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
    }

    private async Task RebuildIndexAsync(string indexName)
    {
        try
        {
            await TablesClient.RebuildIndexAsync(DatabaseName, TableName, indexName);
            _errorMessage = null;
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
    }

    private async Task RebuildAllAsync()
    {
        try
        {
            await TablesClient.RebuildAllIndexesAsync(DatabaseName, TableName);
            _errorMessage = null;
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
    }
}
