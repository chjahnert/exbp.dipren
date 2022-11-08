
using System.Numerics;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements key arithmetics for the <see cref="BigInteger"/> type.
    /// </summary>
    public class BigIntegerKeyArithmetics : IKeyArithmetics<BigInteger>
    {
        private IComparer<BigInteger> _comparer;


        /// <summary>
        ///   Gets the default instance of the <see cref="BigIntegerKeyArithmetics"/> class.
        /// </summary>
        /// <value>
        ///   An <see cref="BigIntegerKeyArithmetics"/> object that is the default instance for the
        ///   <see cref="BigInteger"/> type.
        /// </value>
        public static BigIntegerKeyArithmetics Default { get; } = new BigIntegerKeyArithmetics(Comparer<BigInteger>.Default);


        /// <summary>
        ///   Initializes a new instance of the <see cref="BigIntegerKeyArithmetics"/> class.
        /// </summary>
        /// <param name="comparer">
        ///   The <see cref="IComparable{T}"/> of <see cref="int"/> object to use to compare key values; or
        ///   <see langword="null"/> to use the default comparer.
        /// </param>
        public BigIntegerKeyArithmetics(IComparer<BigInteger> comparer = null)
        {
            this._comparer = (comparer ?? Comparer<BigInteger>.Default);
        }


        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="BigInteger"/> values to split.
        /// </param>
        /// <param name="created">
        ///   A variable that receives the new <see cref="Range{TKey}"/> of <see cref="BigInteger"/> object.
        /// </param>
        /// <returns>
        ///   A <see cref="Range{TKey}"/> of <see cref="BigInteger"/> object that is the updated
        ///   <paramref name="range"/>.
        /// </returns>
        public Range<BigInteger> Split(Range<BigInteger> range, out Range<BigInteger> created)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            BigInteger distance = BigInteger.Abs(range.Last - range.First);

            Range<BigInteger> result = range;

            if (((range.IsInclusive == true) && (distance >= 2)) || ((range.IsInclusive == false) && (distance >= 3)))
            {
                BigInteger half = (distance / 2);

                bool ascending = range.IsAscending(this._comparer);

                if (ascending == false)
                {
                    half *= -1;
                }

                result = new Range<BigInteger>(range.First, range.First + half, false);
                created = new Range<BigInteger>(range.First + half, range.Last, range.IsInclusive);
            }
            else
            {
                created = null;
            }

            return result;
        }
    }
}
