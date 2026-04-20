using System.Linq.Expressions;

namespace CatDb.General.Extensions
{
    public delegate int DecimalGetDigitsDelegate(ref Decimal value);
    public delegate void DecimalWriteDelegate(ref Decimal value, int[] buffer, int index);

    public class DecimalHelper
    {
        public static readonly DecimalHelper Instance = new();

        public readonly DecimalGetDigitsDelegate GetDigits;

        /// <summary>
        /// Writes a decimal value into an int[] array in the same order as a BinaryWriter - d.lo, d.mid, d.hi, d.flags.
        /// </summary>
        //public readonly DecimalWriteDelegate Write;

        /// <summary>
        /// Create decimal from lo,mid,hi,flags
        /// </summary>
        public readonly Func<int, int, int, int, decimal> Constructor;

        public DecimalHelper()
        {
            var lambda = CreateGetDigitsMethod();
            GetDigits = lambda.Compile();

            //Write = CreateWriteMethod().Compile();
            Constructor = CreateConstructorMethod().Compile();
        }

        public Expression<DecimalWriteDelegate> CreateWriteMethod()
        {
            var value = Expression.Parameter(typeof(Decimal).MakeByRefType(), "value");
            var buffer = Expression.Parameter(typeof(int[]), "buffer");
            var index = Expression.Parameter(typeof(int), "index");

            var lo = Expression.Field(value, "_lo64");
            var mid = Expression.Field(value, "mid");
            var hi = Expression.Field(value, "_hi32");
            var flags = Expression.Field(value, "_flags");

            Expression block = Expression.Block(
                Expression.Assign(Expression.ArrayAccess(buffer, Expression.PostIncrementAssign(index)), lo),
                Expression.Assign(Expression.ArrayAccess(buffer, Expression.PostIncrementAssign(index)), mid),
                Expression.Assign(Expression.ArrayAccess(buffer, Expression.PostIncrementAssign(index)), hi),
                Expression.Assign(Expression.ArrayAccess(buffer, Expression.PostIncrementAssign(index)), flags)
            );

            var lambda = Expression.Lambda<DecimalWriteDelegate>(block, value, buffer, index);

            //private void Write(ref Decimal value, int[] buffer, int index)
            //{
            //    buffer[index++] = value.lo;
            //    buffer[index++] = value.mid;
            //    buffer[index++] = value.hi;
            //    buffer[index++] = value.flags;
            //}

            return lambda;
        }

        private Expression<Func<int, int, int, int, decimal>> CreateConstructorMethod()
        {
            var lo = Expression.Parameter(typeof(int), "lo");
            var mid = Expression.Parameter(typeof(int), "mid");
            var hi = Expression.Parameter(typeof(int), "hi");
            var flags = Expression.Parameter(typeof(int), "flags");

            // Use the public decimal(int[]) constructor: new decimal(new[] { lo, mid, hi, flags })
            var decimalConstructor = typeof(decimal).GetConstructor(new[] { typeof(int[]) })!;
            var bitsArray = Expression.NewArrayInit(typeof(int), lo, mid, hi, flags);
            var constructor = Expression.New(decimalConstructor, bitsArray);

            return Expression.Lambda<Func<int, int, int, int, decimal>>(constructor, lo, mid, hi, flags);
        }

        private Expression<DecimalGetDigitsDelegate> CreateGetDigitsMethod()
        {
            var value = Expression.Parameter(typeof(Decimal).MakeByRefType(), "value");

            // Dereference ref Decimal into a local variable before calling GetBits (ref params can't be passed
            // directly to value-type parameters via Expression.Call in .NET 10)
            var local = Expression.Variable(typeof(decimal), "d");
            var getBitsMethod = typeof(Decimal).GetMethod(nameof(Decimal.GetBits), new[] { typeof(Decimal) })!;
            var bits = Expression.Call(getBitsMethod, local);
            var flags = Expression.ArrayIndex(bits, Expression.Constant(3));
            var digits = Expression.RightShift(
                Expression.And(flags, Expression.Constant(0x00FF0000, typeof(int))),
                Expression.Constant(16, typeof(int)));

            // return (Decimal.GetBits(d)[3] & 0x00FF0000) >> 16
            var body = Expression.Block(new[] { local },
                Expression.Assign(local, value),
                digits);

            return Expression.Lambda<DecimalGetDigitsDelegate>(body, value);
        }
    }

    public static class DecimalExtensions
    {
        public static int GetDigits(this Decimal value)
        {
            return DecimalHelper.Instance.GetDigits(ref value);
        }

        public static void Write(this Decimal value, int[] buffer, int index)
        {
            var bits = Decimal.GetBits(value);
            buffer[0] = bits[0];
            buffer[1] = bits[1];
            buffer[2] = bits[2];
            buffer[3] = bits[3];
            //DecimalHelper.Instance.Write(ref value, buffer, index);
        }
    }
}
