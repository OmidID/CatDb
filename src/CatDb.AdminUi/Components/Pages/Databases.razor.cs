using CatDb.AdminUi.Clients;
using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;
using Microsoft.AspNetCore.Components;

namespace CatDb.AdminUi.Components.Pages;

public partial class Databases : ComponentBase
{
    [Inject] private IAdminDatabasesClient DatabasesClient { get; set; } = default!;

    private PagedResult<DatabaseRecord>? _result;
    private string? _errorMessage;
    private string _newDatabaseName = "";
    private bool _isLoading = true;
    private bool _isCreating;
    private int _page = 1;
    private const int _pageSize = 20;

    protected override Task OnInitializedAsync() => LoadAsync();

    private async Task LoadAsync()
    {
        _isLoading = true;
        try
        {
            _result = await DatabasesClient.ListAsync(_page, _pageSize);
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

    private Task OnPageChangedAsync(int page)
    {
        _page = page;
        return LoadAsync();
    }

    private async Task CreateAsync()
    {
        if (string.IsNullOrWhiteSpace(_newDatabaseName))
            return;

        _isCreating = true;
        try
        {
            await DatabasesClient.CreateAsync(_newDatabaseName.Trim());
            _newDatabaseName = "";
            await LoadAsync();
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
        finally
        {
            _isCreating = false;
        }
    }

    private async Task DeleteAsync(string databaseName)
    {
        try
        {
            await DatabasesClient.DeleteAsync(databaseName);
            await LoadAsync();
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
    }
}
