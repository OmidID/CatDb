namespace CatDb.General.Extensions
{
    public enum OnMergeConflict
    {
        Skip,
        ReturnFirst,
        ReturnSecond,
        ReturnFirstAndSecond,
        ReturnByFunction
    }

    public static class EnumerableExtensions
    {
        public static IEnumerable<T> Merge<T>(this IEnumerable<T> collection1, IEnumerable<T> collection2, IComparer<T> comparer, OnMergeConflict onConflict = OnMergeConflict.ReturnFirstAndSecond, Func<T, T, T> function = null)
        {
            using var enumerator1 = collection1.GetEnumerator();
            using var enumerator2 = collection2.GetEnumerator();

            var haveNext1 = enumerator1.MoveNext();
            var haveNext2 = enumerator2.MoveNext();

            if (haveNext1 && haveNext2)
            {
                var item1 = enumerator1.Current;
                var item2 = enumerator2.Current;

                while (true)
                {
                    var cmp = comparer.Compare(item1, item2);
                    if (cmp < 0)
                    {
                        yield return item1;

                        haveNext1 = enumerator1.MoveNext();
                        if (!haveNext1)
                            break;

                        item1 = enumerator1.Current;
                    }
                    else if (cmp > 0)
                    {
                        yield return item2;

                        haveNext2 = enumerator2.MoveNext();
                        if (!haveNext2)
                            break;

                        item2 = enumerator2.Current;
                    }
                    else
                    {
                        switch (onConflict)
                        {
                            case OnMergeConflict.ReturnFirstAndSecond:
                                {
                                    yield return item1;
                                    yield return item2;
                                }
                                break;

                            case OnMergeConflict.ReturnFirst:
                                {
                                    yield return item1;
                                }
                                break;

                            case OnMergeConflict.ReturnSecond:
                                {
                                    yield return item2;
                                }
                                break;

                            case OnMergeConflict.ReturnByFunction:
                                {
                                    yield return function(item1, item2);
                                }
                                break;

                            //case OnMergeConflict.Skip:
                            //    {
                            //    }
                            //    break;
                        }

                        haveNext1 = enumerator1.MoveNext();
                        haveNext2 = enumerator2.MoveNext();

                        if (!haveNext1 || !haveNext2)
                            break;

                        item1 = enumerator1.Current;
                        item2 = enumerator2.Current;
                    }
                }
            }

            while (haveNext1)
            {
                yield return enumerator1.Current;
                haveNext1 = enumerator1.MoveNext();
            }

            while (haveNext2)
            {
                yield return enumerator2.Current;
                haveNext2 = enumerator2.MoveNext();
            }
        }

        public static IEnumerable<T> Merge<T>(this IEnumerable<T> enumerable1, IEnumerable<T> enumerable2, OnMergeConflict onMergeConflict = OnMergeConflict.ReturnFirstAndSecond, Func<T, T, T> function = null)
        {
            return Merge(enumerable1, enumerable2, Comparer<T>.Default, onMergeConflict, function);
        }

        public static IEnumerable<T> Apply<T>(this IEnumerable<T> collection, Action<T> action)
        {
            if (action == null)
                throw new ArgumentNullException("action");

            foreach (var item in collection)
            {
                action(item);

                yield return item;
            }
        }

        public static bool IsOrdered<T>(this IEnumerable<T> collection, IComparer<T> comparer, bool strictMonotone = false)
        {
            using var enumerator = collection.GetEnumerator();
            if (!enumerator.MoveNext())
                return true;

            var limit = strictMonotone ? -1 : 0;
            var item = enumerator.Current;

            while (enumerator.MoveNext())
            {
                var current = enumerator.Current;
                if (comparer.Compare(item, current) > limit)
                    return false;

                item = current;
            }

            return true;
        }

        public static bool IsOrdered<T>(this IEnumerable<T> collection, bool strictMonotone = false)
        {
            return collection.IsOrdered(Comparer<T>.Default, strictMonotone);
        }
    }
}
