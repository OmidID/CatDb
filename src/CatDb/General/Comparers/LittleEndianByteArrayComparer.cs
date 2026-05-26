using System.Runtime.CompilerServices;

namespace CatDb.General.Comparers;
public class LittleEndianByteArrayComparer : IComparer<byte[]>
{
    public static readonly LittleEndianByteArrayComparer Instance = new();
    
    public unsafe int Compare(byte[] x, byte[] y, int length)
    {
        fixed (byte* px = x, py = y)
        {
            var len = length >> 3;
            var remainder = length & 7;

            var p1 = (ulong*)px;
            var p2 = (ulong*)py;

            var i = len;

            if (remainder > 0)
            {
                var shift = (sizeof(ulong) - remainder) << 3;
                var v1 = (p1[i] << shift) >> shift;
                var v2 = (p2[i] << shift) >> shift;
                if (v1 < v2)
                    return -1;
                if (v1 > v2)
                    return 1;
            }

            i--;

            while (i >= 0)
            {
                var v1 = p1[i];
                var v2 = p2[i];
                if (v1 < v2)
                    return -1;
                if (v1 > v2)
                    return 1;

                i--;
            }
        }

        return 0;
    }

    public unsafe int Compare(byte[]? x, byte[]? y)
    {
        if (x is null || y is null) return x is null && y is null ? 0 : x is null ? -1 : 1;
        if (x.Length == y.Length)
            return Compare(x, y, x.Length);

        fixed (byte* px = x, py = y)
        {
            for (int i = x.Length - 1, j = y.Length - 1, len = Math.Min(x.Length, y.Length); len > 0; i--, j--, len--)
            {
                if (px[i] < py[j])
                    return -1;
                if (px[i] > py[j])
                    return 1;
            }
        }

        if (x.Length < y.Length)
            return -1;
        if (x.Length > y.Length)
            return 1;

        return 0;
    }
}
