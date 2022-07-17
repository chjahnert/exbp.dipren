
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Defines a range of keys.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of the key.
    /// </typeparam>
    public class Range<TKey> where TKey : IComparable<TKey>
    {
        private readonly TKey _first;
        private readonly TKey _last;
        private readonly bool _inclusive;


        /// <summary>
        ///   Gets the first key of the current range.
        /// </summary>
        /// <value>
        ///   A <see cref="TKey"/> value that is the first key in the range.
        /// </value>
        /// <remarks>
        ///   The key does not have to actually exist.
        /// </remarks>
        public TKey First => this._first;

        /// <summary>
        ///   Gets the key at which to start processing.
        /// </summary>
        /// <value>
        ///   A <see cref="TKey"/> value that is the key at which to start processing.
        /// </value>
        /// <remarks>
        ///   The key does not have to actually exist.
        /// </remarks>
        public TKey Last => this._last;

        /// <summary>
        ///   Gets a value indicating whether the current range is including <see cref="Last"/>.
        /// </summary>
        /// <value>
        ///   <see langword="true"/> if <see cref="Last"/> is included in the current range; otherwise,
        ///   <see langword="false"/>.
        /// </value>
        public bool IsInclusive => this._inclusive;

        /// <summary>
        ///   Gets a value indicating whether the keys in current range are in ascending order.
        /// </summary>
        /// <value>
        ///   <see langword="true"/> if the keys in the current range are in ascending order; otherwise,
        ///   <see langword="false"/>.
        /// </value>
        public bool IsAscending => this.First.CompareTo(this.Last) <= 0;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Range{TKey}"/> type.
        /// </summary>
        /// <param name="first">
        ///   The first value of the range.
        /// </param>
        /// <param name="last">
        ///   The last value of the range.
        /// </param>
        public Range(TKey first, TKey last, bool inclusive = true)
        {
            Assert.ArgumentIsNotNull(first, nameof(first));
            Assert.ArgumentIsNotNull(last, nameof(last));

            this._first = first;
            this._last = last;
            this._inclusive = inclusive;
        }
    }
}
