using Microsoft.AspNetCore.Localization;

namespace CatDb.AdminUi.Resources;

/// <summary>Languages CatDb.AdminUi ships translations for. Add a code+name here and a matching
/// SharedResources.{code}.resx to support another language.</summary>
public static class SupportedCultures
{
    public const string Default = "en";

    public static readonly (string Code, string DisplayName)[] All =
    [
        ("en", "English"),
        ("es", "Español"),
        ("fr", "Français"),
        ("de", "Deutsch"),
        ("fa", "فارسی"),
        ("he", "עברית"),
    ];

    public static RequestLocalizationOptions BuildRequestLocalizationOptions()
    {
        var codes = All.Select(c => c.Code).ToArray();
        return new RequestLocalizationOptions()
            .SetDefaultCulture(Default)
            .AddSupportedCultures(codes)
            .AddSupportedUICultures(codes);
    }

    /// <summary>
    /// Drives the automatic dir="rtl"/"ltr" on &lt;html&gt; (App.razor) — CultureInfo.TextInfo
    /// already knows which cultures are right-to-left, so this isn't a hand-maintained list.
    /// </summary>
    public static bool IsRightToLeft(System.Globalization.CultureInfo culture) => culture.TextInfo.IsRightToLeft;
}
