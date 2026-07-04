using CatDb.AdminUi.Infrastructure;
using CatDb.AdminUi.Options;
using CatDb.AdminUi.Resources;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Localization;
using Microsoft.Extensions.Options;
using System.ComponentModel.DataAnnotations;

namespace CatDb.AdminUi.Components.Pages;

public partial class Login : ComponentBase
{
    private readonly LoginModel _model = new();
    private string? _errorMessage;
    private bool _isSubmitting;

    [Inject] private CatDbAuthenticationStateProvider AuthStateProvider { get; set; } = default!;
    [Inject] private ApiConnectionState Connection { get; set; } = default!;
    [Inject] private IOptions<CatDbApiOptions> ApiOptions { get; set; } = default!;
    [Inject] private IStringLocalizer<SharedResources> Localizer { get; set; } = default!;
    [Inject] private NavigationManager Navigation { get; set; } = default!;

    [SupplyParameterFromQuery]
    private string? ReturnUrl { get; set; }

    protected override void OnInitialized()
    {
        // Server address defaults from appsettings ("CatDbApi:BaseUrl") but is editable per sign-in —
        // ApiConnectionState is what every typed client actually resolves requests against.
        _model.ServerAddress = Connection.BaseUrl;
    }

    private async Task SubmitAsync()
    {
        _isSubmitting = true;
        _errorMessage = null;
        try
        {
            Connection.BaseUrl = string.IsNullOrWhiteSpace(_model.ServerAddress)
                ? ApiOptions.Value.BaseUrl
                : _model.ServerAddress.Trim();

            var signedIn = await AuthStateProvider.SignInAsync(_model.UserName, _model.Password);
            if (!signedIn)
            {
                _errorMessage = Localizer["Login_InvalidCredentials"];
                return;
            }

            // forceLoad must stay false: it would tear down the SignalR circuit and, with it,
            // the scoped ApiCredentialProvider/ApiConnectionState we just populated.
            Navigation.NavigateTo(string.IsNullOrEmpty(ReturnUrl) ? "/" : ReturnUrl);
        }
        catch (ApiException ex)
        {
            _errorMessage = Localizer["Login_ConnectionError", ex.Message];
        }
        catch (HttpRequestException ex)
        {
            _errorMessage = Localizer["Login_ConnectionError", ex.Message];
        }
        finally
        {
            _isSubmitting = false;
        }
    }

    private sealed class LoginModel
    {
        [Required]
        public string ServerAddress { get; set; } = "";

        [Required]
        public string UserName { get; set; } = "";

        [Required]
        public string Password { get; set; } = "";
    }
}
