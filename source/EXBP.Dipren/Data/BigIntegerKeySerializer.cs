
using System.Globalization;
using System.Numerics;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Implements a default key serializer for the <see cref="BigInteger"/> type.
    /// </summary>
    public sealed class BigIntegerKeySerializer : IKeySerializer<BigInteger>
    {
        /// <summary>
        ///   A ready to be used instance of the <see cref="BigIntegerKeySerializer"/> class.
        /// </summary>
        public static readonly BigIntegerKeySerializer Default = new BigIntegerKeySerializer();


        /// <summary>
        ///   Converts the specified <see cref="BigInteger"/> value to its string representation.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="BigInteger"/> value to convert to a <see cref="string"/>.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value containing the string representation of <paramref name="value"/>.
        /// </returns>
        public string Serialize(BigInteger value)
            => value.ToString(CultureInfo.InvariantCulture);

        /// <summary>
        ///   Converts the <see cref="string"/> representation of a <see cref="BigInteger"/> value to its original
        ///   value.
        /// </summary>
        /// <param name="value">
        ///   The string representation of the <see cref="BigInteger"/> value.
        /// </param>
        /// <returns>
        ///   A <see cref="BigInteger"/> value converted from <paramref name="value"/>.
        /// </returns>
        public BigInteger Deserialize(string value)
            => BigInteger.Parse(value, CultureInfo.InvariantCulture);

    }
}
