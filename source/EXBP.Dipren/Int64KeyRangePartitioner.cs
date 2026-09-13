
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a partitioner that computes boundaries for 64-bit signed integer key ranges.
    /// </summary>
    public class Int64KeyRangePartitioner : IRangePartitioner<long>
    {
        private readonly IComparer<long> _comparer;


        /// <summary>
        ///   Gets the default instance of the <see cref="Int64KeyRangePartitioner"/> class.
        /// </summary>
        /// <value>
        ///   An <see cref="Int64KeyRangePartitioner"/> object that uses the default comparer for the <see cref="long"/>
        ///   type.
        /// </value>
        public static Int64KeyRangePartitioner Default { get; } = new Int64KeyRangePartitioner(Comparer<long>.Default);


        /// <summary>
        ///   Initializes a new instance of the <see cref="Int64KeyRangePartitioner"/> class.
        /// </summary>
        /// <param name="comparer">
        ///   The <see cref="IComparer{T}"/> of <see cref="long"/> to use when comparing key values; or
        ///   <see langword="null"/> to use the default comparer.
        /// </param>
        public Int64KeyRangePartitioner(IComparer<long> comparer = null)
        {
            this._comparer = (comparer ?? Comparer<long>.Default);
        }


        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="long"/> to split.
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
        public Task<RangePartitioningResult<long>> SplitAsync(Range<long> range, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            RangePartitioningResult<long> result;

            bool ascending = range.IsAscending(this._comparer);

            ulong firstValue = unchecked((ulong) range.First);
            ulong lastValue = unchecked((ulong) range.Last);
            ulong distance = ascending ? lastValue - firstValue : firstValue - lastValue;

            if (((range.IsInclusive == true) && (distance >= 2)) || ((range.IsInclusive == false) && (distance >= 3)))
            {
                ulong half = distance / 2;

                if (((distance % 2) == 1) && ((half % 2) == 1))
                {
                    half++;
                }

                ulong value = ascending ? firstValue + half : firstValue - half;
                long midpoint = unchecked((long) value);

                Range<long> updated = new Range<long>(range.First, midpoint, false);
                Range<long> created = new Range<long>(midpoint, range.Last, range.IsInclusive);

                result = new RangePartitioningResult<long>(updated, new Range<long>[] { created });
            }
            else
            {
                result = new RangePartitioningResult<long>(range);
            }

            return Task.FromResult(result);
        }
    }
}
