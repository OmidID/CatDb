using System.Collections.Concurrent;

namespace CatDb.WaterfallTree
{
    public class TypeCache
    {
        private static readonly ConcurrentDictionary<string, Type> Cache = new();

        public static Type GetType(string fullName)
        {
            var type = Type.GetType(fullName, false);
            if (type != null)
                return type;

            return Cache.GetOrAdd(fullName, x =>
            {
                foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                {
                    type = assembly.GetType(fullName);
                    if (type != null)
                        return type;
                }

                return null; //once return null - always return null
            });
        }
    }
}
