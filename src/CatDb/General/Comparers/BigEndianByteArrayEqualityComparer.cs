using CatDb.General.Extensions;

namespace CatDb.General.Comparers;
public class BigEndianByteArrayEqualityComparer : IEqualityComparer<byte[]>
{
    public static readonly BigEndianByteArrayEqualityComparer Instance = new();
    
    public bool Equals(byte[]? x, byte[]? y)
    {
        if (x is null || y is null) return x is null && y is null;
        return x.AsSpan().SequenceEqual(y.AsSpan());
    }

    public int GetHashCode(byte[]? obj)
    {
        if (obj is null) return 0;
        return obj.GetHashCodeEx();
    }
}
