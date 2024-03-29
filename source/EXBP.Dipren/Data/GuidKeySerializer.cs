﻿
namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Implements a default key serializer for the <see cref="Guid"/> type.
    /// </summary>
    public sealed class GuidKeySerializer : IKeySerializer<Guid>
    {
        /// <summary>
        ///   A ready to be used instance of the <see cref="Int32KeySerializer"/> class.
        /// </summary>
        public static readonly GuidKeySerializer Default = new GuidKeySerializer();


        /// <summary>
        ///   Converts the specified integer value to its string representation.
        /// </summary>
        /// <param name="value">
        ///   The integer value to convert to a <see cref="string"/>.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value containing the string representation of <paramref name="value"/>.
        /// </returns>
        public string Serialize(Guid value)
            => value.ToString("d");

        /// <summary>
        ///   Converts the <see cref="string"/> representation of an integer value to its original value.
        /// </summary>
        /// <param name="value">
        ///   The string representation of the integer value.
        /// </param>
        /// <returns>
        ///   An <see cref="int"/> value converted from <paramref name="value"/>.
        /// </returns>
        public Guid Deserialize(string value)
            => Guid.Parse(value);

    }
}
