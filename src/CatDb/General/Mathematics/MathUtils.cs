using CatDb.General.Extensions;

namespace CatDb.General.Mathematics
{
    public static class MathUtils
    {
        private const int SIGN_MASK = ~Int32.MinValue;

        /// <summary>
        /// Returns the number of digits after the decimal point.
        /// </summary>
        public static int GetDigits(decimal value)
        {
            return DecimalHelper.Instance.GetDigits(ref value);
        }

        /// <summary>
        /// Returns the number of digits after the decimal point;
        /// </summary>
        public static int GetDigits(double value)
        {
            var val = (decimal)value;
            var tmp = (double)val;
            if (tmp != value)
                return -1;

            return DecimalHelper.Instance.GetDigits(ref val);
        }

        /// <summary>
        /// Returns the number of digits after the decimal point;
        /// </summary>
        public static int GetDigits(float value)
        {
            return GetDigits((double)value);
        }
    }
}
