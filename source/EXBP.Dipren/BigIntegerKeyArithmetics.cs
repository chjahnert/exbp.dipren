
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
        ///   The <see cref="Range{TKey}"/> of <see cref="BigInteger"/> to split.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> object that represents the asynchronous operation.
        /// </returns>
        public Task<RangePartitioningResult<BigInteger>> SplitAsync(Range<BigInteger> range, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            RangePartitioningResult<BigInteger> result;

            BigInteger distance = BigInteger.Abs(range.Last - range.First);

            if (((range.IsInclusive == true) && (distance >= 2)) || ((range.IsInclusive == false) && (distance >= 3)))
            {
                BigInteger half = (distance / 2);

                bool ascending = range.IsAscending(this._comparer);

                if (ascending == false)
                {
                    half *= -1;
                }

                Range<BigInteger> updated = new Range<BigInteger>(range.First, (range.First + half), false);
                Range<BigInteger> created = new Range<BigInteger>((range.First + half), range.Last, range.IsInclusive);

                result = new RangePartitioningResult<BigInteger>(updated, new Range<BigInteger>[] { created });
            }
            else
            {
                result = new RangePartitioningResult<BigInteger>(range, new Range<BigInteger>[0]);
            }

            return Task.FromResult(result);
        }
    }
}
