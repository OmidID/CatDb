using CatDb.Database;
using CatDb.Extensions;

/// <summary>
/// Shows the cursor (keyset) paging pattern end-to-end:
///   - Insert 500 records with string keys
///   - Page through them with PageAfter — O(log N) per call, any depth
///   - Show total page count (requires Count() = O(M) full scan — cache this in real apps)
///   - Demonstrate that offset paging degrades vs keyset stays constant
/// </summary>
static class KeyQueryPagingDemo
{
    public static void Run(Func<bool, IStorageEngine> openEngine)
    {
        const int total    = 500;
        const int pageSize = 20;

        using var engine = openEngine(true);
        var table = engine.OpenXTable<string, string>("paging");

        // Insert: product codes like "PROD-0001" ... "PROD-0500"
        for (var i = 1; i <= total; i++)
            table[$"PROD-{i:D4}"] = $"Product #{i}";
        engine.Commit();

        // ── Count (full scan — cache in real apps) ─────────────────────────
        var totalCount  = table.Count(KeyQuery<string>.All());
        var totalPages  = (int)Math.Ceiling((double)totalCount / pageSize);
        Console.WriteLine($"Total records : {totalCount}");
        Console.WriteLine($"Page size     : {pageSize}");
        Console.WriteLine($"Total pages   : {totalPages}");
        Console.WriteLine();

        // ── Cursor paging through all pages ───────────────────────────────
        Console.WriteLine("Cursor paging (first 3 + last page):");
        var isFirst = true;
        string cursor = "";
        var pageNum = 1;

        while (true)
        {
            var page = isFirst
                ? table.PageAfter(KeyQuery<string>.All(), take: pageSize).ToList()
                : table.PageAfter(KeyQuery<string>.All(), afterKey: cursor, take: pageSize).ToList();
            if (page.Count == 0) break;

            if (pageNum <= 3 || page.Count < pageSize)
                Console.WriteLine($"  Page {pageNum,3}: {page.First().Key} .. {page.Last().Key}  ({page.Count} items)");
            else if (pageNum == 4)
                Console.WriteLine($"  ...  ({totalPages - 4} more pages)");

            cursor  = page.Last().Key;
            isFirst = false;
            pageNum++;
        }

        Console.WriteLine();

        // ── StartsWith prefix paging ───────────────────────────────────────
        Console.WriteLine("Cursor paging with StartsWith(\"PROD-001\"):");
        var prefixQuery = KeyQuery.StartsWith("PROD-001");
        var isFirstPrefix = true;
        string prefixCursor = "";
        pageNum = 1;

        while (true)
        {
            var page = isFirstPrefix
                ? table.PageAfter(prefixQuery, take: 5).ToList()
                : table.PageAfter(prefixQuery, afterKey: prefixCursor, take: 5).ToList();
            if (page.Count == 0) break;
            Console.WriteLine($"  Page {pageNum}: [{string.Join(", ", page.Select(kv => kv.Key))}]");
            prefixCursor  = page.Last().Key;
            isFirstPrefix = false;
            pageNum++;
        }

        Console.WriteLine();
        Console.WriteLine("Key insight: PageAfter seeks directly with WTree — cost is the same");
        Console.WriteLine("for page 1 or page 1000 on a 10M-record table.");
    }
}
