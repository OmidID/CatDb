using System.Text.Json;
using System.Text.Json.Nodes;
using CatDb.AdminUi.Clients;
using CatDb.AdminUi.Contracts;
using CatDb.AdminUi.Infrastructure;
using Microsoft.AspNetCore.Components;

namespace CatDb.AdminUi.Components.Pages;

public partial class DataBrowser : ComponentBase
{
    [Parameter] public string DatabaseName { get; set; } = "";
    [Parameter] public string TableName { get; set; } = "";

    [Inject] private ITablesClient TablesClient { get; set; } = default!;
    [Inject] private IDataClient DataClient { get; set; } = default!;

    private TableInfo? _table;
    private List<SchemaField> _valueFields = [];
    private string _keyJsonType = "string";
    private string? _errorMessage;
    private bool _isLoadingSchema = true;
    private bool _isSearching;
    private bool _isSavingRecord;
    private string _activeTab = "browse";

    // Browse tab
    private int _browseTake = 50;
    private string _browseFromKey = "";
    private string _browseToKey = "";
    private string _browseDirection = "forward";
    private BrowseResult? _browseResult;

    // Query tab
    private readonly List<FilterRow> _filters = [];
    private readonly List<SortRow> _sorts = [];
    private string _keyFrom = "";
    private string _keyTo = "";
    private int? _skip;
    private int? _take = 50;
    private bool _count;
    private QueryResult? _queryResult;

    private RecordFormModel? _recordForm;

    protected override async Task OnInitializedAsync()
    {
        _isLoadingSchema = true;
        try
        {
            _table = await TablesClient.GetAsync(DatabaseName, TableName);
            _valueFields = SchemaInfo.GetFields(_table.ValueSchema);
            _keyJsonType = SchemaInfo.GetType(_table.KeySchema);
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
        finally
        {
            _isLoadingSchema = false;
        }
    }

    private void AddFilterRow() =>
        _filters.Add(new FilterRow { Field = _valueFields.FirstOrDefault()?.Name ?? "" });

    private async Task BrowseAsync()
    {
        _isSearching = true;
        try
        {
            _browseResult = await DataClient.BrowseAsync(
                DatabaseName, TableName, _browseTake,
                string.IsNullOrWhiteSpace(_browseFromKey) ? null : _browseFromKey,
                string.IsNullOrWhiteSpace(_browseToKey) ? null : _browseToKey,
                _browseDirection);
            _errorMessage = null;
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
        finally
        {
            _isSearching = false;
        }
    }

    private async Task RunQueryAsync()
    {
        _isSearching = true;
        try
        {
            var request = new DataQueryRequest
            {
                AndConditions = _filters
                    .Where(f => !string.IsNullOrWhiteSpace(f.Field))
                    .Select(f => new FilterCondition(f.Field, f.Op, FieldJsonType(f.Field), f.Value, f.Value2))
                    .ToList(),
                Order = _sorts.Select(s => new SortSpec(s.Field, s.Desc)).ToList(),
                Skip = _skip,
                Take = _take,
                Count = _count,
                KeyFrom = string.IsNullOrWhiteSpace(_keyFrom) ? null : _keyFrom,
                KeyTo = string.IsNullOrWhiteSpace(_keyTo) ? null : _keyTo,
                KeyJsonType = _keyJsonType,
            };

            _queryResult = await DataClient.QueryAsync(DatabaseName, TableName, request);
            _errorMessage = null;
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
        finally
        {
            _isSearching = false;
        }
    }

    private string FieldJsonType(string field) =>
        _valueFields.FirstOrDefault(f => f.Name == field)?.JsonType ?? "string";

    private void StartInsert() => _recordForm = new RecordFormModel { IsNew = true };

    private void StartEdit(KeyValueRow row) => _recordForm = new RecordFormModel
    {
        IsNew = false,
        KeyJson = row.Key.GetRawText(),
        ValueJson = row.Value.GetRawText(),
    };

    private async Task SaveRecordAsync()
    {
        if (_recordForm is null)
            return;

        _isSavingRecord = true;
        try
        {
            JsonNode key = JsonNode.Parse(_recordForm.KeyJson) ?? throw new JsonException("Key JSON is empty.");
            JsonNode value = JsonNode.Parse(_recordForm.ValueJson) ?? throw new JsonException("Value JSON is empty.");

            if (_recordForm.IsNew)
                await DataClient.InsertAsync(DatabaseName, TableName, key, value);
            else
                await DataClient.ReplaceAsync(DatabaseName, TableName, key, value);

            _recordForm = null;
            _errorMessage = null;
            await RefreshActiveResultsAsync();
        }
        catch (JsonException ex)
        {
            _errorMessage = $"Invalid JSON: {ex.Message}";
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
        finally
        {
            _isSavingRecord = false;
        }
    }

    private async Task DeleteRecordAsync(JsonElement key)
    {
        try
        {
            var keyNode = JsonNode.Parse(key.GetRawText())!;
            await DataClient.DeleteRecordAsync(DatabaseName, TableName, keyNode);
            _errorMessage = null;
            await RefreshActiveResultsAsync();
        }
        catch (ApiException ex)
        {
            _errorMessage = ex.Message;
        }
    }

    private Task RefreshActiveResultsAsync() => _activeTab == "browse"
        ? _browseResult is not null ? BrowseAsync() : Task.CompletedTask
        : _queryResult is not null ? RunQueryAsync() : Task.CompletedTask;

    private sealed class FilterRow
    {
        public string Field { get; set; } = "";
        public string Op { get; set; } = FilterOps.Equal;
        public string? Value { get; set; }
        public string? Value2 { get; set; }
    }

    private sealed class SortRow
    {
        public string Field { get; set; } = "$key";
        public bool Desc { get; set; }
    }

    private sealed class RecordFormModel
    {
        public bool IsNew { get; set; }
        public string KeyJson { get; set; } = "";
        public string ValueJson { get; set; } = "";
    }
}
