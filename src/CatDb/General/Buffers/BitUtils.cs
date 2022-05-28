namespace CatDb.General.Buffers
{
    public static class BitUtils
    {
        private const int CACHE_SIZE = 2048; //2^11

        private static readonly int[] Cache = new int[CACHE_SIZE];

        static BitUtils()
        {
            for (var i = 0; i < CACHE_SIZE; i++)
                Cache[i] = GetBitBoundsClassic((ulong)i);
        }

        private static int GetBitBoundsClassic(ulong value)
        {
            return (value > 0) ? (int)Math.Ceiling(Math.Log(value + 1.0, 2)) : 1;
        }

        public static int GetBitBounds(ulong value)
        {
            var bits = 0;

            while (value >= CACHE_SIZE) //2^11
            {
                value = value >> 11;
                bits += 11;
            }

            return bits + Cache[value];
        }

        public static int GetBit(byte map, int bitIndex)
        {
            return (map >> (bitIndex & 7)) & 1;
        }

        public static byte SetBit(byte map, int bitIndex, int value)
        {
            var bitMask = 1 << (bitIndex & 7);
            if (value != 0)
                return map |= (byte)bitMask;
            else
                return map &= (byte)(~bitMask);
        }
    }
}
