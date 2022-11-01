
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements extension method for the <see cref="Range{TKey}"/> type.
    /// </summary>
    public static class RangeExtensions
    {
        /// <summary>
        ///   Returns a value indicating whether the keys in current range are in ascending order.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of the keys.
        /// </typeparam>
        /// <param name="range">
        ///   The current key range.
        /// </param>
        /// <param name="comparer">
        ///   A <see cref="IComparer{T}"/> of <typeparamref name="TKey"/> that is used to determine if the current key
        ///   range is in ascending or descending order.
        /// </param>
        /// <returns>
        ///   <see langword="true"/> if the keys in the current key range are in ascending order; otherwise,
        ///   <see langword="false"/>.
        /// </returns>
        public static bool IsAscending<TKey>(this Range<TKey> range, IComparer<TKey> comparer) where TKey : IComparable<TKey>
        {
            Assert.ArgumentIsNotNull(range, nameof(range));
            Assert.ArgumentIsNotNull(comparer, nameof(comparer));

            bool result = (0 >= comparer.Compare(range.First, range.Last));

            return result;
        }
    }
}
