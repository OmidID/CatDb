using CatDb.Data;

namespace CatDb.WaterfallTree
{
    public partial class WTree
    {
        public struct FullKey : IComparable<FullKey>, IEquatable<FullKey>
        {
            public readonly Locator Locator;
            public readonly IData Key;

            public FullKey(Locator locator, IData key)
            {
                Locator = locator;
                Key = key;
            }

            public override string ToString()
            {
                return $"Locator = {Locator}, Key = {Key}";
            }

            #region IComparable<Locator> Members

            public int CompareTo(FullKey other)
            {
                var cmp = Locator.CompareTo(other.Locator);
                if (cmp != 0)
                    return cmp;

                return Locator.KeyComparer.Compare(Key, other.Key);
            }

            #endregion

            #region IEquatable<Locator> Members

            public override int GetHashCode()
            {
                return Locator.GetHashCode() ^ Key.GetHashCode();
            }

            public bool Equals(FullKey other)
            {
                if (!Locator.Equals(other.Locator))
                    return false;

                return Locator.KeyEqualityComparer.Equals(Key, other.Key);
            }

            #endregion
        }
    }
}
