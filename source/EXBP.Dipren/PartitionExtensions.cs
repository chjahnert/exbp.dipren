
using System.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements internal extension methods for the <see cref="Partition"/> and <see cref="Partition{TKey}"/>
    ///   types.
    /// </summary>
    internal static class PartitionExtensions
    {
        /// <summary>
        ///   Converts a <see cref="Partition{TKey}"/> object to a <see cref="PartitionEntry"/> object.
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
        ///   A <see cref="PartitionEntry"/> object that is equivalent to <paramref name="source"/>.
        /// </returns>
        internal static PartitionEntry ToEntry<TKey>(this Partition<TKey> source, IKeySerializer<TKey> serializer) where TKey : IComparable<TKey>
        {
            Debug.Assert(source != null);
            Debug.Assert(serializer != null);

            string first = serializer.Serialize(source.Range.First);
            string last = serializer.Serialize(source.Range.Last);
            string position = serializer.Serialize(source.Position);

            PartitionEntry result = new PartitionEntry(source.Id, source.JobId, source.Owner, source.Created, source.Updated, first, last, source.Range.IsInclusive, position, source.Processed, source.Remaining, source.IsSplitRequested);

            return result;
        }
    }
}
