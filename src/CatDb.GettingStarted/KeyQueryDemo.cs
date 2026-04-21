using CatDb.Database;
using CatDb.Extensions;

/// <summary>
/// Shows all KeyQuery factory methods with a small dataset.
/// Designed so every result is visible on screen.
/// Works identically against a local file or a remote CatDb.Server.
/// </summary>
static class KeyQueryDemo
{
    public static void Run(Func<bool, IStorageEngine> openEngine)
    {
        using var engine = openEngine(true);

        // ── Integer table: keys 1..20 ──────────────────────────────────────
        var ints = engine.OpenXTable<int, string>("demo_ints");
        for (var i = 1; i <= 20; i++) ints[i] = $"value_{i}";

        // ── String table: fruit names ──────────────────────────────────────
        var fruits = engine.OpenXTable<string, string>("demo_fruits");
        foreach (var s in new[]
            { "apple", "apricot", "avocado", "banana", "blueberry",
              "cherry", "date", "elderberry", "fig", "grape" })
            fruits[s] = s.ToUpper();

        engine.Commit();

        Show("AtLeast(10)",          ints.Query(KeyQuery<int>.AtLeast(10)).Select(kv => (object)kv.Key));
        Show("GreaterThan(10)",      ints.Query(KeyQuery<int>.GreaterThan(10)).Select(kv => (object)kv.Key));
        Show("AtMost(5)",            ints.Query(KeyQuery<int>.AtMost(5)).Select(kv => (object)kv.Key));
        Show("LessThan(5)",          ints.Query(KeyQuery<int>.LessThan(5)).Select(kv => (object)kv.Key));
        Show("Between(8, 12)",       ints.Query(KeyQuery<int>.Between(8, 12)).Select(kv => (object)kv.Key));
        Show("Between(8,12 excl.)",  ints.Query(KeyQuery<int>.Between(8, 12, fromInclusive: false, toInclusive: false)).Select(kv => (object)kv.Key));
        Show("All (capped 5)",       ints.Query(KeyQuery<int>.All()).Take(5).Select(kv => (object)kv.Key));
        Console.WriteLine();
        Show("StartsWith(\"a\")",         fruits.Query(KeyQuery.StartsWith("a")).Select(kv => (object)kv.Key));
        Show("StartsWith(\"b\")",         fruits.Query(KeyQuery.StartsWith("b")).Select(kv => (object)kv.Key));
        Show("Backward Between(8,12)",   ints.QueryBackward(KeyQuery<int>.Between(8, 12)).Select(kv => (object)kv.Key));
        Show("Backward StartsWith(\"a\")", fruits.QueryBackward(KeyQuery.StartsWith("a")).Select(kv => (object)kv.Key));
    }

    static void Show(string label, IEnumerable<object> keys)
        => Console.WriteLine($"  {label,-35} → [{string.Join(", ", keys)}]");
}
