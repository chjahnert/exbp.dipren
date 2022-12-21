
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements key arithmetics for the <see cref="long"/> type.
    /// </summary>
    public class Int64KeyArithmetics : IKeyArithmetics<long>
    {
        private readonly IComparer<long> _comparer;


        /// <summary>
        ///   Gets the default instance of the <see cref="Int64KeyArithmetics"/> class.
        /// </summary>
        /// <value>
        ///   An <see cref="Int32KeyArithmetics"/> object that uses the default comparer for the <see cref="long"/>
        ///   type.
        /// </value>
        public static Int64KeyArithmetics Default { get; } = new Int64KeyArithmetics(Comparer<long>.Default);


        /// <summary>
        ///   Initializes a new instance of the <see cref="Int64KeyArithmetics"/> class.
        /// </summary>
        /// <param name="comparer">
        ///   The <see cref="IComparable{T}"/> of <see cref="long"/> object to use to compare key values; or
        ///   <see langword="null"/> to use the default comparer.
        /// </param>
        public Int64KeyArithmetics(IComparer<long> comparer = null)
        {
            this._comparer = (comparer ?? Comparer<long>.Default);
        }


        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="long"/> values to split.
        /// </param>
        /// <param name="created">
        ///   A variable that receives the new <see cref="Range{TKey}"/> of <see cref="long"/> object created.
        /// </param>
        /// <returns>
        ///   A <see cref="Range{TKey}"/> of <see cref="long"/> object that is the updated value of
        ///   <paramref name="range"/>.
        /// </returns>
        public Range<long> Split(Range<long> range, out Range<long> created)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            Range<long> result = range;
            created = null;

            double distance = Math.Abs(((double) range.Last) - ((double) range.First));

            if (((range.IsInclusive == true) && (distance >= 2)) || ((range.IsInclusive == false) && (distance >= 3)))
            {
                long half = (long) Math.Round(distance / 2);

                bool ascending = range.IsAscending(this._comparer);

                if (ascending == false)
                {
                    half *= -1;
                }

                result = new Range<long>(range.First, range.First + half, false);
                created = new Range<long>(range.First + half, range.Last, range.IsInclusive);
            }

            return result;
        }

        /// <summary>
        ///   Splits the specified integer key range into two ranges.
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
