using System.Windows.Forms;
using CatDb.General.Communication;

namespace CatDb.Server;

public partial class MainForm : Form
{
    private UsersAndExceptionHandler _handler;

    public MainForm()
    {
        InitializeComponent();
        ElementSize();
        MinimizeTray.Visible = false;

        _handler = new UsersAndExceptionHandler();
        _handler.Start();

        startToolStripMenuItem.Enabled = false;
        stopToolStripMenuItem.Enabled  = true;
        Text = "CatDbServer Running!";
    }

    private void ServerForm_FormClosing(object sender, FormClosingEventArgs e)
    {
        if (MessageBox.Show("Are you sure?", "Are you sure?",
                MessageBoxButtons.OKCancel, MessageBoxIcon.Question,
                MessageBoxDefaultButton.Button2) == DialogResult.Cancel)
            e.Cancel = true;

        _handler.Stop();
    }

    private void notifyIcon1_MouseDoubleClick(object sender, MouseEventArgs e)
    {
        Show();
        WindowState = FormWindowState.Normal;
        MinimizeTray.Visible = false;
    }

    private void ServerForm_Resize(object sender, EventArgs e)
    {
        ElementSize();

        if (WindowState == FormWindowState.Minimized)
        {
            MinimizeTray.Visible = true;
            Hide();
        }
        else
            MinimizeTray.Visible = false;
    }

    private void Disconnect_Click(object sender, EventArgs e) =>
        _handler.Disconnect(usersList.SelectedItems);

    private void timer1_Tick(object sender, EventArgs e)
    {
        foreach (var error in _handler.GetExceptions())
        {
            errorList.Items.Insert(0, new ListViewItem([error.Key, error.Value]));
            try   { CatDbService.Service.EventLog.WriteEntry(error.Value, System.Diagnostics.EventLogEntryType.Error); }
            catch { }
        }

        if (errorList.Items.Count == 101)
            errorList.Items.RemoveAt(100);

        var selectedItems = new string[usersList.SelectedItems.Count];
        for (var i = 0; i < usersList.SelectedItems.Count; i++)
            selectedItems[i] = usersList.SelectedItems[i].Text;

        if (_handler.IsFinishRefresh)
        {
            usersList.Items.Clear();
            foreach (var client in _handler.GetClients())
                usersList.Items.Add(client);
        }

        foreach (var select in selectedItems)
        {
            foreach (ListViewItem user in usersList.Items)
            {
                if (user.Text.Equals(select))
                {
                    user.Selected = true;
                    break;
                }
            }
        }

        Disconnect.Enabled = usersList.Items.Count > 0 && !_handler.IsDisconnecting;
    }

    private void exitToolStripMenuItem_Click(object sender, EventArgs e)  => Application.Exit();
    private void aboutToolStripMenuItem_Click(object sender, EventArgs e) => new AboutBox().ShowDialog();

    private void ElementSize()
    {
        User.Width  = Width;
        Time.Width  = Width / 2 - 20;
        Error.Width = Width / 2;
    }

    private void usersList_ItemSelectionChanged(object sender, ListViewItemSelectionChangedEventArgs e) =>
        e.Item.Selected = usersList.Items.Count > 0 && !_handler.IsDisconnecting;

    private void startToolStripMenuItem_Click(object sender, EventArgs e)
    {
        Program.StorageEngineServer.TcpServer.Start();
        Program.StorageEngineServer.Start();
        _handler.Start();
        timer.Start();
        startToolStripMenuItem.Enabled = false;
        stopToolStripMenuItem.Enabled  = true;
        Text = "CatDbServer Running!";
    }

    private void stopToolStripMenuItem_Click(object sender, EventArgs e)
    {
        timer.Stop();
        _handler.Stop();
        Program.StorageEngineServer.Stop();
        Program.StorageEngineServer.TcpServer.Stop();
        usersList.Items.Clear();
        stopToolStripMenuItem.Enabled  = false;
        startToolStripMenuItem.Enabled = true;
        Text = "CatDbServer Stopped!";
    }
}
