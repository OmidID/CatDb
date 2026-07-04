// Deliberately in the ROOT namespace, not CatDb.AdminUi.Resources: IStringLocalizer<T> builds the
// resource base name as "{RootNamespace}.{T's namespace relative to root}.{T's name}". The SDK's
// embedded-resource naming for .resx under Resources/ drops that folder segment (compiles to
// "CatDb.AdminUi.SharedResources.resources", confirmed via Assembly.GetManifestResourceNames — NOT
// "CatDb.AdminUi.Resources.SharedResources..."), so AddLocalization() is called with no ResourcesPath
// and this marker type must sit in the root namespace to match. If it lived in
// CatDb.AdminUi.Resources, every lookup would silently miss and IStringLocalizer would just print
// the raw key back (which is exactly the bug this comment is here to prevent re-introducing).
namespace CatDb.AdminUi;

/// <summary>
/// Marker type only — anchors <c>IStringLocalizer&lt;SharedResources&gt;</c> to the resource set in
/// Resources/SharedResources*.resx (SharedResources.resx = English/default, SharedResources.{culture}.resx
/// = translations). Add a new language by dropping in another SharedResources.{culture}.resx with the
/// same keys and registering its code in <see cref="CatDb.AdminUi.Resources.SupportedCultures"/>.
/// </summary>
public sealed class SharedResources;
