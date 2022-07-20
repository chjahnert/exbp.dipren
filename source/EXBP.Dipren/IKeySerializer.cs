
namespace EXBP.Dipren
{
    /// <summary>
    ///   Allows a type to implement a key serializer that can handle convert key values to strings and vice versa.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of the key.
    /// </typeparam>
    public interface IKeySerializer<TKey>
    {
        /// <summary>
        ///   Converts the specified key value to its string representation.
        /// </summary>
        /// <param name="key">
        ///   The key to convert to a <see cref="string"/>.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value containing the string representation of <paramref name="key"/>.
        /// </returns>
        string Searialize(TKey key);

        /// <summary>
        ///   Converts the <see cref="string"/> representation of a key to its original value.
        /// </summary>
        /// <param name="value">
        ///   The string representation of the key.
        /// </param>
        /// <returns>
        ///   A <typeparamref name="TKey"/> value converted from <paramref name="value"/>.
        /// </returns>
        TKey Deserailize(string value);
    }
}
