using System.Collections;

namespace CatDb.General.Extensions
{
    public static class ListExtensions
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
            return BinarySearch(array, index, length, value, Comparer<T>.Default);
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
        
          public static T[] GetArray<T>(this List<T> instance)
        {
            return ListHelper<T>.Instance.GetArray(instance);
        }

        public static void SetArray<T>(this List<T> instance, T[] array)
        {
            ListHelper<T>.Instance.SetArray(instance, array);
        }

        public static void SetCount<T>(this List<T> instance, int count)
        {
            ListHelper<T>.Instance.SetCount(instance, count);
        }

        public static void IncrementVersion<T>(this List<T> instance)
        {
            ListHelper<T>.Instance.IncrementVersion(instance);
        }

        /// <summary>
        /// Splits the list into two parts, where the right part contains count elements and returns the right part of the list.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="instance"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static List<T> Split<T>(this List<T> instance, int count)
        {
            if (instance.Count < count)
                throw new ArgumentException("list.Count < count");

            var list = new List<T>(instance.Capacity);
            Array.Copy(instance.GetArray(), instance.Count - count, list.GetArray(), 0, count);
            list.SetCount(count);
            instance.SetCount(instance.Count - count);
            instance.IncrementVersion();

            return list;
        }

        public static void AddRange<T>(this List<T> instance, T[] array, int index, int count)
        {
            var newCount = instance.Count + count;

            if (instance.Capacity < newCount)
                instance.Capacity = newCount;

            Array.Copy(array, index, instance.GetArray(), instance.Count, count);
            instance.SetCount(newCount);
            instance.IncrementVersion();
        }

        public static void AddRange<T>(this List<T> instance, List<T> list, int index, int count)
        {
            instance.AddRange(list.GetArray(), index, count);
        }
    }
}
