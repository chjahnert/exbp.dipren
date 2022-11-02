﻿
using System.Data.Common;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data.SQLite
{
    /// <summary>
    ///   Implements extension methods for the <see cref="DbDataReader"/> class.
    /// </summary>
    internal static class DbDataReaderExtensions
    {
        /// <summary>
        ///   Gets the value of the specified column as a <see cref="string"/> value.
        /// </summary>
        /// <param name="reader">
        ///   The current <see cref="DbDataReader"/> instance.
        /// </param>
        /// <param name="name">
        ///   The name of the column.
        /// </param>
        /// <returns>
        ///   The <see cref="string"/> value in the specified column; or <see langword="null"/> if not set.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="reader"/> is a <see langword="null"/> reference.
        /// </exception>
        internal static string GetNullableString(this DbDataReader reader, string name)
        {
            Assert.ArgumentIsNotNull(reader, nameof(reader));

            int ordinal = reader.GetOrdinal(name);
            bool isNull = reader.IsDBNull(ordinal);

            string result = null;

            if (isNull == false)
            {
                result = reader.GetString(ordinal);
            }

            return result;
        }
    }
}