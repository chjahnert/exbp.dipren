
using System.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements extension methods for the <see cref="PartitionEntry"/> type.
    /// </summary>
    internal static class PartitionEntryExtensions
    {
        /// <summary>
        ///   Converts a <see cref="PartitionEntry"/> object to a <see cref="Partition{TKey}"/> object.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of keys.
        /// </typeparam>
        /// <param name="source">
        ///   The <see cref="PartitionEntry"/> object to convert.
        /// </param>
        /// <param name="serializer">
        ///   The key serializer to use.
        /// </param>
        /// <returns>
        ///   A <see cref="Partition{TKey}"/> object that is equivalent to <paramref name="source"/>.
        /// </returns>
        internal static Partition<TKey> ToPartition<TKey>(this PartitionEntry source, IKeySerializer<TKey> serializer) where TKey : IComparable<TKey>
        {
            Debug.Assert(source != null);
            Debug.Assert(serializer != null);

            TKey first = serializer.Deserialize(source.First);
            TKey last = serializer.Deserialize(source.Last);
            TKey position = serializer.Deserialize(source.Position);

            Range<TKey> range = new Range<TKey>(first, last, source.IsInclusive);

            Partition<TKey> result = new Partition<TKey>(source.Id, source.JobId, source.Owner, source.Created, source.Updated, range, position, source.Processed, source.Remaining);

            return result;
        }
    }
}
