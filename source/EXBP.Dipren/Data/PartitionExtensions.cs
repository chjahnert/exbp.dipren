
using System.Diagnostics;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Implements extension methods for the <see cref="Partition"/> type.
    /// </summary>
    internal static class PartitionExtensions
    {
        /// <summary>
        ///   Converts a <see cref="Partition"/> object to a <see cref="Partition{TKey}"/> object.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of keys.
        /// </typeparam>
        /// <param name="source">
        ///   The <see cref="Partition"/> object to convert.
        /// </param>
        /// <param name="serializer">
        ///   The key serializer to use.
        /// </param>
        /// <returns>
        ///   A <see cref="Partition{TKey}"/> object that is equivalent to <paramref name="source"/>.
        /// </returns>
        internal static Partition<TKey> ToPartition<TKey>(this Partition source, IKeySerializer<TKey> serializer)
        {
            Debug.Assert(source != null);
            Debug.Assert(serializer != null);

            TKey first = serializer.Deserialize(source.First);
            TKey last = serializer.Deserialize(source.Last);
            TKey position = (source.Position != null) ? serializer.Deserialize(source.Position) : default;

            Range<TKey> range = new Range<TKey>(first, last, source.IsInclusive);

            Partition<TKey> result = new Partition<TKey>(source.Id, source.JobId, source.Owner, source.Created, source.Updated, range, position, source.Processed, source.Remaining, source.IsCompleted, source.Throughput, source.IsSplitRequested);

            return result;
        }
    }
}
