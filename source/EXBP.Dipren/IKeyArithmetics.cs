
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
        ///   Splits the specified range into two or more ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> to split.
        /// </param>
        /// <param name="updated">
        ///   A variable that receives the updated <paramref name="range"/> object.
        /// </param>
        /// <returns>
        ///   An array of <see cref="Range{TKey}"/> objects that are the new ranges created.
        /// </returns>
        Range<TKey>[] Split(Range<TKey> range, out Range<TKey> updated);
    }
}
