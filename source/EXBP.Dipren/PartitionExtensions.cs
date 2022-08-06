﻿
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
        internal static Partition Flatten<TKey>(this Partition<TKey> source, IKeySerializer<TKey> serializer) where TKey : IComparable<TKey>
        {
            Debug.Assert(source != null);
            Debug.Assert(serializer != null);

            string first = serializer.Serialize(source.Range.First);
            string last = serializer.Serialize(source.Range.Last);
            string position = serializer.Serialize(source.Position);

            Partition result = new Partition(source.Id, source.JobId, source.Owner, source.Created, source.Updated, first, last, source.Range.IsInclusive, position, source.Processed, source.Remaining);

            return result;
        }

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
        internal static Partition<TKey> Restore<TKey>(this Partition source, IKeySerializer<TKey> serializer) where TKey : IComparable<TKey>
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
