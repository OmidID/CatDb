using System.Reflection;
using System.Windows.Forms;

namespace CatDb.Server;

partial class AboutBox : Form
{
    public AboutBox()
    {
        InitializeComponent();
        Text                       = $"About {AssemblyTitle}";
        labelProductName.Text      = AssemblyProduct;
        labelVersion.Text          = $"Version {AssemblyVersion}";
        labelCopyright.Text        = AssemblyCopyright;
        labelCompanyName.Text      = AssemblyCompany;
        textBoxDescription.Text    = AssemblyDescription;
    }

    private static Assembly Asm => Assembly.GetExecutingAssembly();

    public string AssemblyTitle =>
        Asm.GetCustomAttribute<AssemblyTitleAttribute>()?.Title is { Length: > 0 } t
            ? t
            : Path.GetFileNameWithoutExtension(Asm.Location);

    public string AssemblyVersion    => Asm.GetName().Version?.ToString() ?? "";
    public string AssemblyDescription => Asm.GetCustomAttribute<AssemblyDescriptionAttribute>()?.Description ?? "";
    public string AssemblyProduct    => Asm.GetCustomAttribute<AssemblyProductAttribute>()?.Product ?? "";
    public string AssemblyCopyright  => Asm.GetCustomAttribute<AssemblyCopyrightAttribute>()?.Copyright ?? "";
    public string AssemblyCompany    => Asm.GetCustomAttribute<AssemblyCompanyAttribute>()?.Company ?? "";
}
