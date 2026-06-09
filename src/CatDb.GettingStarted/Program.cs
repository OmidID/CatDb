// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Diagnostics;
using CatDb.Data;
using CatDb.General.Extensions;
using DatabaseBenchmark;
using CatDb.Database;
using Database = CatDb.Database;

// ── Switch between local file and remote server ───────────────────────────────
const bool   USE_SERVER  = true;          // true = connect to CatDb.Server
const string SERVER_USERNAME = "admin";
const string SERVER_PASSWORD = "admin";
const string SERVER_HOST = "localhost";
const int    SERVER_PORT = 7182;
const string FILE_NAME   = "test.CatDb";
// ─────────────────────────────────────────────────────────────────────────────

#pragma warning disable CS0162 // const-bool branch — flip USE_SERVER to enable
IStorageEngine OpenEngine(bool fresh = false)
{
    if (USE_SERVER)
    {
        Console.WriteLine($"Connecting to server {SERVER_HOST}:{SERVER_PORT}...");
        return Database.CatDb.FromNetwork(
	        SERVER_HOST,
	        SERVER_PORT,
	        Path.GetFileNameWithoutExtension(FILE_NAME),
	        SERVER_USERNAME,
	        SERVER_PASSWORD);
    }
    if (fresh) File.Delete(FILE_NAME);
    return Database.CatDb.FromFile(FILE_NAME);
}
#pragma warning restore CS0162

// ── Register all demos ────────────────────────────────────────────────────────
var demos = new List<Demo>
{
    new("Basic insert & read (1M records)",
        () => BasicDemo.Run(OpenEngine)),

    new("KeyQuery — range search demo",
        () => KeyQueryDemo.Run(OpenEngine)),

    new("KeyQuery — performance on 2M records",
        () => KeyQueryPerfDemo.Run(OpenEngine)),

    new("KeyQuery — cursor (keyset) paging demo",
        () => KeyQueryPagingDemo.Run(OpenEngine)),

    new("Secondary indexes — unique & non-unique",
        () => SecondaryIndexDemo.Run(OpenEngine)),

    new("Ordering — filter then sort by another index/key",
        () => SortDemo.Run(OpenEngine)),
};

// ── Menu loop ─────────────────────────────────────────────────────────────────
while (true)
{
    Console.Clear();
    Console.WriteLine("═══════════════════════════════════════════");
    Console.WriteLine("  CatDb Getting Started");
    Console.WriteLine($"  Mode: {(USE_SERVER ? $"Server ({SERVER_HOST}:{SERVER_PORT})" : "Local file")}");
    Console.WriteLine("═══════════════════════════════════════════");
    Console.WriteLine();

    for (var i = 0; i < demos.Count; i++)
        Console.WriteLine($"  [{i + 1}] {demos[i].Title}");

    Console.WriteLine("  [0] Exit");
    Console.WriteLine();
    Console.Write("Select: ");

    var input = Console.ReadLine()?.Trim();
    if (!int.TryParse(input, out var choice) || choice < 0 || choice > demos.Count)
    {
        Console.WriteLine("Invalid choice. Press any key...");
        Console.ReadKey();
        continue;
    }

    if (choice == 0) break;

    Console.Clear();
    Console.WriteLine($"▶ {demos[choice - 1].Title}");
    Console.WriteLine(new string('─', 60));

    try
    {
        var sw = Stopwatch.StartNew();
        demos[choice - 1].Action();
        sw.Stop();
        Console.WriteLine();
        Console.WriteLine($"Done in {sw.Elapsed.TotalSeconds:F2}s");
    }
    catch (Exception ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"Error: {ex.Message}");
        Console.ResetColor();
    }

    Console.WriteLine();
    Console.Write("Press any key to return to menu...");
    Console.ReadKey();
}

record Demo(string Title, Action Action);
