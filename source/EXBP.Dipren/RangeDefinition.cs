
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Defines a range of keys.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of the key.
    /// </typeparam>
    public class RangeDefinition<TKey> where TKey : IComparable<TKey>
    {
        private readonly TKey _first;
        private readonly TKey _last;


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
        ///   Initializes a new instance of the <see cref="RangeDefinition{TKey}"/> type.
        /// </summary>
        /// <param name="first">
        ///   The first value of the range.
        /// </param>
        /// <param name="last">
        ///   The last value of the range.
        /// </param>
        public RangeDefinition(TKey first, TKey last)
        {
            Assert.ArgumentIsNotNull(first, nameof(first));
            Assert.ArgumentIsNotNull(last, nameof(last));

            this._first = first;
            this._last = last;
        }   
    }
}
