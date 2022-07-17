
namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements operations for manipulating key ranges.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of keys.
    /// </typeparam>
    public interface IKeyArithmetics<TKey> where TKey : IComparable<TKey>
    {
        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> to split.
        /// </param>
        /// <returns>
        ///   An array of <see cref="Range{TKey}"/> objects.
        /// </returns>
        Range<TKey>[] Split(Range<TKey> range);
    }
}
