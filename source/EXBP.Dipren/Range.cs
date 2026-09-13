
using System.Diagnostics;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Defines a range of keys.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of the key.
    /// </typeparam>
    [DebuggerDisplay("Range = [{First}..{Last}], Inclusive = {IsInclusive}")]
    public class Range<TKey>
    {
        private readonly TKey _first;
        private readonly TKey _last;
        private readonly bool _inclusive;


        /// <summary>
        ///   Gets the first key of the current range.
        /// </summary>
        /// <value>
        ///   A <typeparamref name="TKey"/> value that is the first key in the range.
        /// </value>
        /// <remarks>
        ///   The key is a boundary and does not need to exist in the data source.
        /// </remarks>
        public TKey First => this._first;

        /// <summary>
        ///   Gets the last key of the current range.
        /// </summary>
        /// <value>
        ///   A <typeparamref name="TKey"/> value that is the last key in the range.
        /// </value>
        /// <remarks>
        ///   The key is a boundary and does not need to exist in the data source.
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
        ///   Initializes a new instance of the <see cref="Range{TKey}"/> type.
        /// </summary>
        /// <param name="first">
        ///   The first value of the range.
        /// </param>
        /// <param name="last">
        ///   The last value of the range.
        /// </param>
        /// <param name="inclusive">
        ///   <see langword="true"/> if <paramref name="last"/> is included in the range; otherwise,
        ///   <see langword="false"/>.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   <paramref name="first"/> or <paramref name="last"/> is <see langword="null"/>.
        /// </exception>
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
