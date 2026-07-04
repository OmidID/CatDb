using CatDb.AdminUi.Clients;
using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;
using Microsoft.AspNetCore.Components;

namespace CatDb.AdminUi.Components.Pages;

public partial class Users : ComponentBase
{
    [Inject] private IAdminUsersClient UsersClient { get; set; } = default!;

    private PagedResult<UserView>? _result;
    private string? _errorMessage;
    private bool _isLoading = true;
    private bool _isSaving;
    private bool _isNew;
    private UserFormModel? _editing;
    private int _page = 1;
    private const int _pageSize = 20;

    protected override Task OnInitializedAsync() => LoadAsync();

    private async Task LoadAsync()
    {
        _isLoading = true;
        try
        {
            _result = await UsersClient.ListAsync(_page, _pageSize);
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

    private void StartCreate()
    {
        _isNew = true;
        _editing = new UserFormModel();
    }

    private void StartEdit(UserView user)
    {
        _isNew = false;
        _editing = new UserFormModel
        {
            UserName = user.UserName,
            GlobalFlags = [.. user.GlobalPermissions.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)],
            DatabaseRows =
            [
                .. DatabasePermissionsFormat.Parse(user.DatabasePermissions).Select(kv => new DbPermissionRow
                {
                    Database = kv.Key,
                    Flags = [.. kv.Value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)],
                }),
            ],
        };
    }

    private static void ToggleFlag(HashSet<string> flags, string flag, object? isChecked)
    {
        if (isChecked is true)
            flags.Add(flag);
        else
            flags.Remove(flag);
    }

    private async Task SaveAsync()
    {
        if (_editing is null)
            return;

        _isSaving = true;
        try
        {
            var request = new UpsertUserRequest(
                _editing.UserName.Trim(),
                _editing.Password,
                _editing.GlobalFlags.Count == 0 ? "None" : string.Join(", ", _editing.GlobalFlags),
                _editing.DatabaseRows
                    .Where(r => !string.IsNullOrWhiteSpace(r.Database))
                    .ToDictionary(r => r.Database.Trim(), r => r.Flags.Count == 0 ? "None" : string.Join(", ", r.Flags)));

            await UsersClient.UpsertAsync(request);
            _editing = null;
            await LoadAsync();
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
        finally
        {
            _isSaving = false;
        }
    }

    private async Task DeleteAsync(string userName)
    {
        try
        {
            await UsersClient.DeleteAsync(userName);
            await LoadAsync();
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
    }

    private sealed class UserFormModel
    {
        public string UserName { get; set; } = "";
        public string Password { get; set; } = "";
        public HashSet<string> GlobalFlags { get; set; } = [];
        public List<DbPermissionRow> DatabaseRows { get; set; } = [];
    }

    private sealed class DbPermissionRow
    {
        public string Database { get; set; } = "";
        public HashSet<string> Flags { get; set; } = [];
    }
}
