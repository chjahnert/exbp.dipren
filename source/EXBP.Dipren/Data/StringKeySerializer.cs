
namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Implements a default key serializer for the <see cref="string"/> type.
    /// </summary>
    /// <remarks>
    ///   This type does not perform any changes to the values passed to it. It will always return the exact same value
    ///   both during serialization and deserialization.
    /// </remarks>
    public sealed class StringKeySerializer : IKeySerializer<string>
    {
        /// <summary>
        ///   A ready to be used instance of the <see cref="Int32KeySerializer"/> class.
        /// </summary>
        public static readonly StringKeySerializer Default = new StringKeySerializer();


        /// <summary>
        ///   Converts the specified string value to its serialized string representation.
        /// </summary>
        /// <param name="value">
        ///   The value to convert to a <see cref="string"/>.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value containing the serialized representation of <paramref name="value"/>.
        /// </returns>
        public string Serialize(string value)
            => value;

        /// <summary>
        ///   Converts the serialized representation of a <see cref="string"/> value to its original value.
        /// </summary>
        /// <param name="value">
        ///   The serialized representation of the string value.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value converted from <paramref name="value"/>.
        /// </returns>
        public string Deserialize(string value)
            => value;

    }
}
