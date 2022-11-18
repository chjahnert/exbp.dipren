
using System.Diagnostics;

using EXBP.Dipren.Data;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements internal extension methods for the <see cref="Partition"/> and <see cref="Partition{TKey}"/>
    ///   types.
    /// </summary>
    internal static class PartitionExtensions
    {
        /// <summary>
        ///   Converts a <see cref="Partition{TKey}"/> object to a <see cref="Partition"/> object.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of keys.
        /// </typeparam>
        /// <param name="source">
        ///   The <see cref="Partition{TKey}"/> object to convert.
        /// </param>
        /// <param name="serializer">
        ///   The key serializer to use.
        /// </param>
        /// <returns>
        ///   A <see cref="Partition"/> object that is equivalent to <paramref name="source"/>.
        /// </returns>
        internal static Partition ToEntry<TKey>(this Partition<TKey> source, IKeySerializer<TKey> serializer)
        {
            Debug.Assert(source != null);
            Debug.Assert(serializer != null);

            string first = serializer.Serialize(source.Range.First);
            string last = serializer.Serialize(source.Range.Last);
            string position = serializer.Serialize(source.Position);

            Partition result = new Partition(source.Id, source.JobId, source.Created, source.Updated, first, last, source.Range.IsInclusive, position, source.Processed, source.Remaining, source.Owner, source.IsCompleted, source.Throughput, source.IsSplitRequested);

            return result;
        }

        /// <summary>
        ///   Returns the key range left to process.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of keys.
        /// </typeparam>
        /// <param name="source">
        ///   The <see cref="Partition{TKey}"/> object for which to get the remaining key range.
        /// </param>
        /// <returns>
        ///   A <see cref="Range{TKey}"/> of <typeparamref name="TKey"/> representing the key range left to process.
        /// </returns>
        internal static Range<TKey> GetRemainingKeyRange<TKey>(this Partition<TKey> source)
        {
            Debug.Assert(source != null);

            TKey first = ((source.Processed > 0L) ? source.Position : source.Range.First);
            Range<TKey> result = new Range<TKey>(first, source.Range.Last, source.Range.IsInclusive);

            return result;
        }
    }
}
