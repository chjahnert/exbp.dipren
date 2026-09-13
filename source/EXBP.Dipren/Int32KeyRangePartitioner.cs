
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a partitioner that computes boundaries for 32-bit signed integer key ranges.
    /// </summary>
    public class Int32KeyRangePartitioner : IRangePartitioner<int>
    {
        private readonly IComparer<int> _comparer;


        /// <summary>
        ///   Gets the default instance of the <see cref="Int32KeyRangePartitioner"/> class.
        /// </summary>
        /// <value>
        ///   An <see cref="Int32KeyRangePartitioner"/> object that uses the default comparer for the <see cref="int"/>
        ///   type.
        /// </value>
        public static Int32KeyRangePartitioner Default { get; } = new Int32KeyRangePartitioner(Comparer<int>.Default);


        /// <summary>
        ///   Initializes a new instance of the <see cref="Int32KeyRangePartitioner"/> class.
        /// </summary>
        /// <param name="comparer">
        ///   The <see cref="IComparer{T}"/> of <see cref="int"/> to use when comparing key values; or
        ///   <see langword="null"/> to use the default comparer.
        /// </param>
        public Int32KeyRangePartitioner(IComparer<int> comparer = null)
        {
            this._comparer = (comparer ?? Comparer<int>.Default);
        }


        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="int"/> to split.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A task whose result contains the updated range and any range created by the split.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   <paramref name="range"/> is <see langword="null"/>.
        /// </exception>
        public Task<RangePartitioningResult<int>> SplitAsync(Range<int> range, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            RangePartitioningResult<int> result;

            bool ascending = range.IsAscending(this._comparer);

            uint firstValue = unchecked((uint) range.First);
            uint lastValue = unchecked((uint) range.Last);
            uint distance = ascending ? lastValue - firstValue : firstValue - lastValue;

            if (((range.IsInclusive == true) && (distance >= 2)) || ((range.IsInclusive == false) && (distance >= 3)))
            {
                uint half = distance / 2;

                if (((distance % 2) == 1) && ((half % 2) == 1))
                {
                    half++;
                }

                uint value = ascending ? firstValue + half : firstValue - half;
                int midpoint = unchecked((int) value);

                Range<int> updated = new Range<int>(range.First, midpoint, false);
                Range<int> created = new Range<int>(midpoint, range.Last, range.IsInclusive);

                result = new RangePartitioningResult<int>(updated, new Range<int>[] { created });
            }
            else
            {
                result = new RangePartitioningResult<int>(range);
            }

            return Task.FromResult(result);
        }
    }
}
