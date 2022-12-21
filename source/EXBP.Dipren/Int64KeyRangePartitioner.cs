
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a partitioner for 64 bit signed integer key ranges that computes the range boundaries.
    /// </summary>
    public class Int64KeyRangePartitioner : IRangePartitioner<long>
    {
        private readonly IComparer<long> _comparer;


        /// <summary>
        ///   Gets the default instance of the <see cref="Int64KeyRangePartitioner"/> class.
        /// </summary>
        /// <value>
        ///   An <see cref="Int32KeyRangePartitioner"/> object that uses the default comparer for the <see cref="long"/>
        ///   type.
        /// </value>
        public static Int64KeyRangePartitioner Default { get; } = new Int64KeyRangePartitioner(Comparer<long>.Default);


        /// <summary>
        ///   Initializes a new instance of the <see cref="Int64KeyRangePartitioner"/> class.
        /// </summary>
        /// <param name="comparer">
        ///   The <see cref="IComparable{T}"/> of <see cref="long"/> object to use to compare key values; or
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
        ///   A <see cref="Task{TResult}"/> object that represents the asynchronous operation.
        /// </returns>
        public Task<RangePartitioningResult<long>> SplitAsync(Range<long> range, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            RangePartitioningResult<long> result;

            double distance = Math.Abs(((double) range.Last) - ((double) range.First));

            if (((range.IsInclusive == true) && (distance >= 2)) || ((range.IsInclusive == false) && (distance >= 3)))
            {
                long half = (long) Math.Round(distance / 2);

                bool ascending = range.IsAscending(this._comparer);

                if (ascending == false)
                {
                    half *= -1;
                }

                Range<long> updated = new Range<long>(range.First, range.First + half, false);
                Range<long> created = new Range<long>(range.First + half, range.Last, range.IsInclusive);

                result = new RangePartitioningResult<long>(updated, new Range<long>[] { created });
            }
            else
            {
                result = new RangePartitioningResult<long>(range, new Range<long>[0]);
            }

            return Task.FromResult(result);
        }
    }
}
