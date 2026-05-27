#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Diagnostics;

namespace CatDb.General.Extensions;
public class SortedSetHelper<T>
{
    public static readonly SortedSetHelper<T> Instance = new();

    private KeyValuePair<bool, T> Find(SortedSet<T> set, T key)
    {
        var node = set.FindNode(key); //private method invocation..
        if (node != null)
            return new KeyValuePair<bool, T>(true, node.Item);

        return new KeyValuePair<bool, T>(false, default(T));
    }

    public void ConstructFromSortedArray(SortedSet<T> set, T[] array, int index, int count)
    {
        set.Root = SortedSet<T>.ConstructRootFromSortedArray(array, index, index + count - 1, null); //private method invocation..
        set.Count = count;
        set.Version++;
    }

    public bool Replace(SortedSet<T> set, T item, Func<T, T, T> onExist)
    {
        if (set.Root == null)
        {
            set.Root = new SortedSet<T>.Node(item, NodeColor.Black);
            set.Count = 1;
            set.Version++;
            return false;
        }

        var root = set.Root;
        SortedSet<T>.Node node = null;
        SortedSet<T>.Node grandParent = null;
        SortedSet<T>.Node greatGrandParent = null;
        set.Version++;
        var cmp = 0;
        var comparer = set.Comparer;

        while (root != null)
        {
            cmp = comparer.Compare(item, root.Item);
            if (cmp == 0)
            {
                set.Root.Color = NodeColor.Black;
                root.Item = onExist != null ? onExist(root.Item, item) : item;
                return true;
            }

            if (root.Is4Node)
            {
                root.Split4Node();
                if (node != null && node.IsRed)
                    set.InsertionBalance(root, ref node, grandParent, greatGrandParent);
            }

            greatGrandParent = grandParent;
            grandParent = node;
            node = root;
            root = (cmp < 0) ? root.Left : root.Right;
        }

        var current = new SortedSet<T>.Node(item, NodeColor.Red);
        if (cmp > 0)
            node.Right = current;
        else
            node.Left = current;

        if (node.IsRed)
            set.InsertionBalance(current, ref node, grandParent, greatGrandParent);

        set.Root.Color = NodeColor.Black;
        set.Count++;
        return false;
    }
    public SortedSet<T> GetViewBetween(SortedSet<T> set, T lowerValue, T upperValue, bool lowerBoundActive, bool upperBoundActive)
    {
        if ((lowerBoundActive && upperBoundActive) && set.Comparer.Compare(lowerValue, upperValue) > 0)
            throw new ArgumentException("lowerBound is greater than upperBound");

        return new SortedSet<T>.TreeSubSet(set, lowerValue, upperValue, lowerBoundActive, upperBoundActive);
    }
    
    public KeyValuePair<bool, T> FindNext(SortedSet<T> set, T item)
    {
        int cmp;
        SortedSet<T>.Node next = null;
        var node = set.Root;

        while (node != null)
        {
            cmp = set.Comparer.Compare(item, node.Item);

            if (cmp == 0)
                return new KeyValuePair<bool, T>(true, node.Item);

            if (cmp < 0)
            {
                next = node;
                node = node.Left;
            }
            else
                node = node.Right;
        }

        if (next != null)
            return new KeyValuePair<bool, T>(true, next.Item);

        return new KeyValuePair<bool, T>(false, default(T));
    }

    public KeyValuePair<bool, T> FindPrev(SortedSet<T> set, T item)
    {
        int cmp;
        SortedSet<T>.Node prev = null;
        var node = set.Root;

        while (node != null)
        {
            cmp = set.Comparer.Compare(item, node.Item);

            if (cmp == 0)
                return new KeyValuePair<bool, T>(true, node.Item);

            if (cmp > 0)
            {
                prev = node;
                node = node.Right;
            }
            else
                node = node.Left;
        }

        if (prev != null)
            return new KeyValuePair<bool, T>(true, prev.Item);

        return new KeyValuePair<bool, T>(false, default(T));
    }

    public KeyValuePair<bool, T> FindAfter(SortedSet<T> set, T item)
    {
        int cmp;
        SortedSet<T>.Node next = null;
        var node = set.Root;

        while (node != null)
        {
            cmp = set.Comparer.Compare(item, node.Item);

            if (cmp == 0)
            {
                if (node.Right != null)
                {
                    var tmp = node.Right;

                    while (tmp != null)
                    {
                        next = tmp;
                        tmp = tmp.Left;
                    }
                }

                break;
            }

            if (cmp < 0)
            {
                next = node;
                node = node.Left;
            }
            else
                node = node.Right;
        }

        if (next != null)
            return new KeyValuePair<bool, T>(true, next.Item);

        return new KeyValuePair<bool, T>(false, default(T));
    }

    public KeyValuePair<bool, T> FindBefore(SortedSet<T> set, T item)
    {
        int cmp;
        SortedSet<T>.Node prev = null;
        var node = set.Root;

        while (node != null)
        {
            cmp = set.Comparer.Compare(item, node.Item);

            if (cmp == 0)
            {
                if (node.Left != null)
                {
                    var tmp = node.Left;

                    while (tmp != null)
                    {
                        prev = tmp;
                        tmp = tmp.Right;
                    }
                }

                break;
            }

            if (cmp > 0)
            {
                prev = node;
                node = node.Right;
            }
            else
                node = node.Left;
        }

        if (prev != null)
            return new KeyValuePair<bool, T>(true, prev.Item);

        return new KeyValuePair<bool, T>(false, default(T));
    }

    public bool TryGetValue(SortedSet<T> set, T key, out T value)
    {
        var kv = Find(set, key);

        if (!kv.Key)
        {
            value = default(T);
            return false;
        }

        value = kv.Value;
        return true;
    }

}

public static class SortedSetExtensions
{
    public static bool TryGetValue<T>(this SortedSet<T> set, T key, out T value)
    {
        return SortedSetHelper<T>.Instance.TryGetValue(set, key, out value);
    }

    /// <summary>
    /// Elements in array must be ordered and unique from the set.Comparer point of view.
    /// </summary>
    public static void ConstructFromSortedArray<T>(this SortedSet<T> set, T[] array, int index, int count)
    {
        SortedSetHelper<T>.Instance.ConstructFromSortedArray(set, array, index, count);
    }

    /// <summary>
    /// Replace an existing element.
    /// 1.If item not exists it will be added to the set.
    /// 2.If item already exist there is two cases:
    ///  - if onExists is null the new item will replace existing item;
    ///  - if onExists is not null the item returned by the onExist(T existingItem, T newItem) function will replace the existing item.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the set.</typeparam>
    /// <param name="set"></param>
    /// <param name="item">The element to add in the set</param>
    /// <param name="onExist">T onExist(T existingItem, T newItem)</param>
    /// <returns>
    /// Returns false if the specified item not exist in the set (insert).
    /// Returns true if the specified item exist in the set (replace).
    /// </returns>
    public static bool Replace<T>(this SortedSet<T> set, T item, Func<T, T, T> onExist)
    {
        return SortedSetHelper<T>.Instance.Replace(set, item, onExist);
    }

    /// <summary>
    /// Replace an existing element.
    /// 1.If item not exists it will be added to the set.
    /// 2.If item already exist the new item will replace existing item;
    /// </summary>
    /// <typeparam name="T">The type of the elements in the set.</typeparam>
    /// <param name="set"></param>
    /// <param name="item">The element to add in the set</param>
    /// <returns>
    /// Returns false if the specified item not exist in the set (insert).
    /// Returns true if the specified item exist in the set (replace).
    /// </returns>
    public static bool Replace<T>(this SortedSet<T> set, T item)
    {
        return set.Replace(item, null);
    }

    public static SortedSet<T> GetViewBetween<T>(this SortedSet<T> set, T lowerValue, T upperValue, bool lowerBoundActive, bool upperBoundActive)
    {
        return SortedSetHelper<T>.Instance.GetViewBetween(set, lowerValue, upperValue, lowerBoundActive, upperBoundActive);
    }

    public static bool FindNext<T>(this SortedSet<T> set, T key, out T value)
    {
        var returnValue = SortedSetHelper<T>.Instance.FindNext(set, key);

        value = returnValue.Value;

        return returnValue.Key;
    }

    public static bool FindPrev<T>(this SortedSet<T> set, T key, out T value)
    {
        var returnValue = SortedSetHelper<T>.Instance.FindPrev(set, key);

        value = returnValue.Value;

        return returnValue.Key;
    }

    public static bool FindAfter<T>(this SortedSet<T> set, T key, out T value)
    {
        var returnValue = SortedSetHelper<T>.Instance.FindAfter(set, key);

        value = returnValue.Value;

        return returnValue.Key;
    }

    public static bool FindBefore<T>(this SortedSet<T> set, T key, out T value)
    {
        var returnValue = SortedSetHelper<T>.Instance.FindBefore(set, key);

        value = returnValue.Value;

        return returnValue.Key;
    }

    /// <summary>
    /// Splits the set into two parts, where the right part contains count number of elements and return the right part of the set.
    /// </summary>
    public static SortedSet<T> Split<T>(this SortedSet<T> set, int count)
    {
        var array = new T[set.Count];
        set.CopyTo(array);

        Debug.Assert(array.IsOrdered(set.Comparer, true));

        set.ConstructFromSortedArray(array, 0, array.Length - count);

        Debug.Assert(set.IsOrdered(set.Comparer, true));

        var right = new SortedSet<T>(set.Comparer);
        right.ConstructFromSortedArray(array, array.Length - count, count);

        Debug.Assert(right.IsOrdered(right.Comparer, true));

        return right;
    }

    public static bool Remove<T>(this SortedSet<T> set, T fromKey, T toKey)
    {
        var cmp = set.Comparer.Compare(fromKey, toKey);
        if (cmp > 0)
            throw new ArgumentException();

        if (cmp == 0)
            return set.Remove(fromKey);

        var arr = new T[set.Count];
        set.CopyTo(arr);

        var from = Array.BinarySearch(arr, fromKey, set.Comparer);
        var fromIdx = from;
        if (fromIdx < 0)
        {
            fromIdx = ~fromIdx;
            if (fromIdx == set.Count)
                return false;
        }

        var to = Array.BinarySearch(arr, fromIdx, set.Count - fromIdx, toKey, set.Comparer);
        var toIdx = to;
        if (toIdx < 0)
        {
            if (from == to)
                return false;

            toIdx = ~toIdx - 1;
        }

        var count = toIdx - fromIdx + 1;
        if (count == 0)
            return false;

        Array.Copy(arr, toIdx + 1, arr, fromIdx, set.Count - (toIdx + 1));

        Debug.Assert(arr.Take(set.Count - count).IsOrdered(set.Comparer, true));

        set.ConstructFromSortedArray(arr, 0, set.Count - count);

        return true;
    }
}
