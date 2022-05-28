using System.Collections;

namespace CatDb.General.Extensions
{
    public static class IListExtensions
    {
        public static int BinarySearch<T>(this IList<T> array, int index, int length, T value, IComparer<T> comparer)
        {
            if (comparer == null)
                throw new ArgumentNullException("comparer");

            var low = index;
            var high = index + length - 1;

            while (low <= high)
            {
                var mid = (low + high) >> 1;
                var cmp = comparer.Compare(array[mid], value);

                if (cmp == 0)
                    return mid;
                if (cmp < 0)
                    low = mid + 1;
                else
                    high = mid - 1;
            }

            return ~low;
        }

        public static int BinarySearch<T>(this IList<T> array, int index, int length, T value)
        {
            return BinarySearch<T>(array, index, length, value, Comparer<T>.Default);
        }

        public static int BinarySearch(this IList array, int index, int length, object value, IComparer comparer)
        {
            if (comparer == null)
                throw new ArgumentNullException("comparer");

            var low = index;
            var high = index + length - 1;

            while (low <= high)
            {
                var mid = (low + high) >> 1;
                var cmp = comparer.Compare(array[mid], value);

                if (cmp == 0)
                    return mid;
                if (cmp < 0)
                    low = mid + 1;
                else
                    high = mid - 1;
            }

            return ~low;
        }

        public static int BinarySearch(this IList array, int index, int length, object value)
        {
            return BinarySearch(array, index, length, Comparer.Default);
        }
    }
}
