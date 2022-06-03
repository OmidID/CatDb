using System.Collections.Concurrent;
using System.Diagnostics;
using CatDb.Data;

namespace CatDb.WaterfallTree
{
    public partial class WTree
    {
        private class BranchesOptimizator
        {
            private const int MAP_CAPACITY = 131072;
            private ConcurrentDictionary<Locator, Range> _map = new();
            private BranchCollection _branches;

            public void Rebuild(BranchCollection branches)
            {
                _branches = branches;
                _map = BuildRanges();
            }

            private ConcurrentDictionary<Locator, Range> BuildRanges()
            {
                var map = new ConcurrentDictionary<Locator, Range>();
                var locator = _branches[0].Key.Locator;
                var range = new Range(0, true);
                map[locator] = range;

                for (var i = 1; i < _branches.Count; i++)
                {
                    var newLocator = _branches[i].Key.Locator;

                    if (newLocator.Equals(locator))
                    {
                        range.LastIndex = i;
                        continue;
                    }

                    locator = newLocator;
                    map[locator] = range = new Range(i, true);
                }

                return map;
            }

            public Range FindRange(Locator locator)
            {
                if (_map.TryGetValue(locator, out var range))
                    return range;

                var idx = _branches.BinarySearch(new FullKey(locator, null));
                Debug.Assert(idx < 0);
                idx = ~idx - 1;
                Debug.Assert(idx >= 0);

                _map[locator] = range = new Range(idx, false);

                if (_map.Count > MAP_CAPACITY)
                    _map = BuildRanges(); //TODO: background rebuild

                return range;
            }

            public int FindIndex(Range range, Locator locator, IData key)
            {
                if (!range.IsBaseLocator)
                    return range.LastIndex;

                var cmp = locator.KeyComparer.Compare(key, _branches[range.LastIndex].Key.Key);
                if (cmp >= 0)
                    return range.LastIndex;

                if (range.FirstIndex == range.LastIndex)
                    return range.LastIndex - 1;

                var idx = _branches.BinarySearch(new FullKey(locator, key), range.FirstIndex, range.LastIndex - range.FirstIndex, LightComparer.Instance);
                if (idx < 0)
                    idx = ~idx - 1;

                return idx;
            }

            private class LightComparer : IComparer<KeyValuePair<FullKey, Branch>>
            {
                public readonly static LightComparer Instance = new();

                public int Compare(KeyValuePair<FullKey, Branch> x, KeyValuePair<FullKey, Branch> y)
                {
                    //Debug.Assert(x.Key.Path.Equals(y.Key.Path));

                    return x.Key.Locator.KeyComparer.Compare(x.Key.Key, y.Key.Key);
                }
            }
        }

        [DebuggerDisplay("FirstIndex = {FirstIndex}, LastIndex = {LastIndex}, IsBaseLocator = {IsBaseLocator}")]
        private class Range
        {
            public int FirstIndex;
            public int LastIndex;
            public bool IsBaseLocator;

            public Range(int firstIndex, bool baseLocator)
            {
                FirstIndex = firstIndex;
                LastIndex = firstIndex;
                IsBaseLocator = baseLocator;
            }
        }
    }
}
