
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a partitioner for 32 bit signed integer key ranges that computes the range boundaries.
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
        ///   The <see cref="IComparable{T}"/> of <see cref="int"/> object to use to compare key values; or
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
        ///   A <see cref="Task{TResult}"/> object that represents the asynchronous operation.
        /// </returns>
        public Task<RangePartitioningResult<int>> SplitAsync(Range<int> range, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            RangePartitioningResult<int> result;

            double distance = Math.Abs(((double) range.Last) - ((double) range.First));

            if (((range.IsInclusive == true) && (distance >= 2)) || ((range.IsInclusive == false) && (distance >= 3)))
            {
                int half = (int) Math.Round(distance / 2);

                bool ascending = range.IsAscending(this._comparer);

                if (ascending == false)
                {
                    half *= -1;
                }

                Range<int> updated = new Range<int>(range.First, range.First + half, false);
                Range<int> created = new Range<int>(range.First + half, range.Last, range.IsInclusive);

                result = new RangePartitioningResult<int>(updated, new Range<int>[] { created });
            }
            else
            {
                result = new RangePartitioningResult<int>(range, new Range<int>[0]);
            }

            return Task.FromResult(result);
        }
    }
}
