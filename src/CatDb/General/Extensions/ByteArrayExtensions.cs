using CatDb.General.Comparers;
using System.Text;

namespace CatDb.General.Extensions
{
    public static class ByteArrayExtensions
    {
        private static readonly string[] hexValues = BitConverter.ToString(Enumerable.Range(0, 256).Select(x => (byte)x).ToArray()).Split('-');

        public static int GetHashCodeEx(this byte[] buffer)
        {
            const int CONSTANT = 17;
            var hashCode = 37;

            var common = new CommonArray();
            common.ByteArray = buffer;
            var array = common.Int32Array;

            var length = buffer.Length;
            var remainder = length & 3;
            var len = length >> 2;

            var i = 0;

            while (i < len)
            {
                hashCode = CONSTANT * hashCode + array[i];
                i++;
            }

            if (remainder > 0)
            {
                var shift = sizeof(uint) - remainder;
                hashCode = CONSTANT * hashCode + ((array[i] << shift) >> shift);
            }

            return hashCode;
        }

        /// <summary>
        /// http://en.wikipedia.org/wiki/MurmurHash
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="seed"></param>
        /// <returns></returns>
        public static int MurMurHash3(this byte[] buffer, int seed = 37)
        {
            const uint c1 = 0xcc9e2d51;
            const uint c2 = 0x1b873593;
            const int r1 = 15;
            const int r2 = 13;
            const uint m = 5;
            const uint n = 0xe6546b64;

            var hash = (uint)seed;

            var common = new CommonArray();
            common.ByteArray = buffer;
            var array = common.UInt32Array;

            var length = buffer.Length;
            var remainder = length & 3;
            var len = length >> 2;

            var i = 0;

            while (i < len)
            {
                var k = array[i];

                k *= c1;
                k = (k << r1) | (k >> (32 - r1)); //k = rotl32(k, r1);
                k *= c2;

                hash ^= k;
                hash = (hash << r2) | (hash >> (32 - r2)); //hash = rotl32(hash, r2);
                hash = hash * m + n;

                i++;
            }

            if (remainder > 0)
            {
                var shift = sizeof(uint) - remainder;
                var k = (array[i] << shift) >> shift;

                k *= c1;
                k = (k << r1) | (k >> (32 - r1)); //k = rotl32(k, r1);
                k *= c2;

                hash ^= k;
            }

            hash ^= (uint)length;

            //hash = fmix(hash);
            hash ^= hash >> 16;
            hash *= 0x85ebca6b;
            hash ^= hash >> 13;
            hash *= 0xc2b2ae35;
            hash ^= hash >> 16;

            return (int)hash;
        }

        public static byte[] Middle(this byte[] buffer, int offset, int length)
        {
            var middle = new byte[length];
            Buffer.BlockCopy(buffer, offset, middle, 0, length);
            return middle;
        }

        public static byte[] Left(this byte[] buffer, int length)
        {
            return buffer.Middle(0, length);
        }

        public static byte[] Right(this byte[] buffer, int length)
        {
            return buffer.Middle(buffer.Length - length, length);
        }

        /// <summary>
        /// Convert byte array to hex string
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public static string ToHex(this byte[] buffer)
        {
            var sb = new StringBuilder(2 * buffer.Length);

            for (var i = 0; i < buffer.Length; i++)
                sb.Append(hexValues[buffer[i]]);

            return sb.ToString();
        }

        public static int GetBit(this byte[] map, int bitIndex)
        {
            return (map[bitIndex >> 3] >> (bitIndex & 7)) & 1;
        }

        public static void SetBit(this byte[] map, int bitIndex, int value)
        {
            var bitMask = 1 << (bitIndex & 7);
            if (value != 0)
                map[bitIndex >> 3] |= (byte)bitMask;
            else
                map[bitIndex >> 3] &= (byte)(~bitMask);
        }
    }
}
 