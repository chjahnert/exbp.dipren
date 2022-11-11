
using System.Globalization;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Implements a default key serializer for the <see cref="Int32"/> type.
    /// </summary>
    public sealed class Int32KeySerializer : IKeySerializer<int>
    {
        /// <summary>
        ///   A ready to be used instance of the <see cref="Int32KeySerializer"/> class.
        /// </summary>
        public static readonly Int32KeySerializer Default = new Int32KeySerializer();


        /// <summary>
        ///   Converts the specified integer value to its string representation.
        /// </summary>
        /// <param name="value">
        ///   The integer value to convert to a <see cref="string"/>.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value containing the string representation of <paramref name="value"/>.
        /// </returns>
        public string Serialize(int value)
            => value.ToString(CultureInfo.InvariantCulture);

        /// <summary>
        ///   Converts the <see cref="string"/> representation of an integer value to its original value.
        /// </summary>
        /// <param name="value">
        ///   The string representation of the integer value.
        /// </param>
        /// <returns>
        ///   An <see cref="int"/> value converted from <paramref name="value"/>.
        /// </returns>
        public int Deserialize(string value)
            => int.Parse(value, CultureInfo.InvariantCulture);

    }
}
