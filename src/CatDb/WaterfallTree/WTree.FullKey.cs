using CatDb.Data;

namespace CatDb.WaterfallTree;

public partial class WTree
{
    public struct FullKey(Locator locator, IData? key) : IComparable<FullKey>, IEquatable<FullKey>
    {
        public readonly Locator Locator = locator;
        public readonly IData?  Key     = key;

        public override string ToString() => $"Locator = {Locator}, Key = {Key}";

        public int CompareTo(FullKey other)
        {
            var cmp = Locator.CompareTo(other.Locator);
            return cmp != 0 ? cmp : Locator.KeyComparer!.Compare(Key, other.Key);
        }

        public override int GetHashCode() => Locator.GetHashCode() ^ (Key?.GetHashCode() ?? 0);

        public bool Equals(FullKey other) =>
            Locator.Equals(other.Locator) && Locator.KeyEqualityComparer!.Equals(Key, other.Key);
    }
}
