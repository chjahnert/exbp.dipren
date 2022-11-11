
using System.Globalization;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Implements a default key serializer for the <see cref="long"/> type.
    /// </summary>
    public sealed class Int64KeySerializer : IKeySerializer<long>
    {
        /// <summary>
        ///   A ready to be used instance of the <see cref="Int64KeySerializer"/> class.
        /// </summary>
        public static readonly Int64KeySerializer Default = new Int64KeySerializer();


        /// <summary>
        ///   Converts the specified <see cref="long"/> value to its string representation.
        /// </summary>
        /// <param name="key">
        ///   The <see cref="long"/> value to convert to a <see cref="string"/>.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value containing the string representation of <paramref name="key"/>.
        /// </returns>
        public string Serialize(long key)
            => key.ToString(CultureInfo.InvariantCulture);

        /// <summary>
        ///   Converts the <see cref="string"/> representation of a <see cref="long"/> value to its original value.
        /// </summary>
        /// <param name="value">
        ///   The string representation of the <see cref="long"/> value.
        /// </param>
        /// <returns>
        ///   A <see cref="long"/> value converted from <paramref name="value"/>.
        /// </returns>
        public long Deserialize(string value)
            => long.Parse(value, CultureInfo.InvariantCulture);

    }
}
