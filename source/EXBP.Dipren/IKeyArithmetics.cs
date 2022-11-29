
namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements operations for manipulating key ranges.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of keys.
    /// </typeparam>
    public interface IKeyArithmetics<TKey>
    {
        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> to split.
        /// </param>
        /// <param name="created">
        ///   A variable that receives the new <paramref name="range"/> object created.
        /// </param>
        /// <returns>
        ///   A <see cref="Range{TKey}"/> object that is the updated value of <paramref name="range"/>.
        /// </returns>
        Range<TKey> Split(Range<TKey> range, out Range<TKey> created);
    }
}
