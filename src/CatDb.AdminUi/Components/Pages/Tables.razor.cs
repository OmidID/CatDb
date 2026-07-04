using System.Text.Json;
using CatDb.AdminUi.Clients;
using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;
using Microsoft.AspNetCore.Components;

namespace CatDb.AdminUi.Components.Pages;

public partial class Tables : ComponentBase
{
    private const string SampleObjectSchema =
        """{ "type": "object", "properties": { "Name": { "type": "string" }, "Age": { "type": "integer", "format": "int32" } } }""";

    [Parameter] public string DatabaseName { get; set; } = "";

    [Inject] private ITablesClient TablesClient { get; set; } = default!;

    private List<TableSummary> _tables = [];
    private string? _errorMessage;
    private bool _isLoading = true;
    private bool _isCreating;
    private bool _showCreate;
    private string _newTableName = "";
    private string _newKeySchemaJson = """{ "type": "string" }""";
    private string _newValueSchemaJson = SampleObjectSchema;

    protected override Task OnInitializedAsync() => LoadAsync();

    private async Task LoadAsync()
    {
        _isLoading = true;
        try
        {
            _tables = await TablesClient.ListAsync(DatabaseName);
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

    private async Task CreateAsync()
    {
        if (string.IsNullOrWhiteSpace(_newTableName))
        {
            _errorMessage = "Table name is required.";
            return;
        }

        _isCreating = true;
        try
        {
            JsonElement keySchema, valueSchema;
            try
            {
                keySchema = ParseSchema(_newKeySchemaJson);
            }
            catch (JsonException ex)
            {
                _errorMessage = $"Invalid key schema JSON: {ex.Message}";
                return;
            }

            try
            {
                valueSchema = ParseSchema(_newValueSchemaJson);
            }
            catch (JsonException ex)
            {
                _errorMessage = $"Invalid value schema JSON: {ex.Message}";
                return;
            }

            await TablesClient.CreateAsync(DatabaseName, new CreateTableRequest(_newTableName.Trim(), keySchema, valueSchema));
            _newTableName = "";
            _showCreate = false;
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

    private static JsonElement ParseSchema(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    private async Task DeleteAsync(string tableName)
    {
        try
        {
            await TablesClient.DeleteAsync(DatabaseName, tableName);
            await LoadAsync();
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
    }
}
