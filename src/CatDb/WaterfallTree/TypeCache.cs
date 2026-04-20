using System.Collections.Concurrent;

namespace CatDb.WaterfallTree;

public static class TypeCache
{
    private static readonly ConcurrentDictionary<string, Type> Cache = new();

    public static Type GetType(string fullName)
    {
        var type = Type.GetType(fullName, false);
        if (type is not null)
            return type;

        return Cache.GetOrAdd(fullName, x =>
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                var t = assembly.GetType(fullName);
                if (t is not null)
                    return t;
            }
            return null;
        });
    }
}
