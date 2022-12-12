
using System.Globalization;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Defines some common character sets for the <see cref="StringKeyArithmetics"/> type.
    /// </summary>
    /// <remarks>
    ///   See the <see href="https://github.com/chjahnert/exbp.dipren/wiki/String-Keys">documentation</see> for a
    ///   better method to define the character set to use with the <see cref="StringKeyArithmetics"/> type.
    /// </remarks>
    public static class StringKeyCharacterSet
    {
        /// <summary>
        ///   Gets the characters that may appear in a hexadecimal value.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains all characters that may appear in a hexadecimal value.
        /// </value>
        /// <remarks>
        ///   The characters are sorted using the invariant culture in a case-sensitive manner. User the
        ///   <see cref="Sort(string, CultureInfo, CompareOptions)"/> method to sort them using different sorting
        ///   rules.
        /// </remarks>
        public static string Hexadecimal => "0123456789AaBbCcDdEeFf";

        /// <summary>
        ///   Gets the characters that may appear in a Base64 encoded value.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains all characters that may appear in a Base64 encoded value.
        /// </value>
        /// <remarks>
        ///   The characters are sorted using the invariant culture in a case-sensitive manner. User the
        ///   <see cref="Sort(string, CultureInfo, CompareOptions)"/> method to sort them using different sorting
        ///   rules.
        /// </remarks>
        public static string Base64 => "/+=0123456789aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ";

        /// <summary>
        ///   Gets the printable characters that are included in the standard ASCII character set.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the printable characters of the standard ASCII character set.
        /// </value>
        /// <remarks>
        ///   The characters are sorted using the invariant culture in a case-sensitive manner. User the
        ///   <see cref="Sort(string, CultureInfo, CompareOptions)"/> method to sort them using different sorting
        ///   rules.
        /// </remarks>
        public static string AsciiStandard => " _-,;:!?.'\"()[]{}@*/\\&#%`^+<=>|~$0123456789aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ";

        /// <summary>
        ///   Gets the printable characters that are included in the extended ASCII character set.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the printable characters of the extended ASCII character set.
        /// </value>
        /// <remarks>
        ///   The characters are sorted using the invariant culture in a case-sensitive manner. User the
        ///   <see cref="Sort(string, CultureInfo, CompareOptions)"/> method to sort them using different sorting
        ///   rules.
        /// </remarks>
        public static string AsciiExtended => "­  _-,;:!¡?¿.·'\"«»()[]{}§¶@*/\\&#%`´^¯¨¸°©®+±÷×<=>¬|¦~¤¢$£¥01¹½¼2²3³¾456789aAªáÁàÀâÂåÅäÄãÃæÆbBcCçÇdDðÐeEéÉèÈêÊëËfFgGhHiIíÍìÌîÎïÏjJkKlLmMnNñÑoOºóÓòÒôÔöÖõÕøØpPqQrRsSßtTuUúÚùÙûÛüÜvVwWxXyYýÝzZþÞµ";


        /// <summary>
        ///   Sorts the characters in the specified string using the string comparison rules defined by the specified
        ///   culture.
        /// </summary>
        /// <param name="value">
        ///   A <see cref="string"/> value defining the character set.
        /// </param>
        /// <param name="culture">
        ///   A <see cref="CultureInfo"/> object that defines culture-sensitive string sorting.
        /// </param>
        /// <param name="options">
        ///   A <see cref="CompareOptions"/> that defines string comparison options.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value containing the distinct list of characters from <paramref name="value"/>,
        ///   sorted using the rules defined by the specified culture and comparison options.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="value"/> or argument <paramref name="culture"/> is a <see langword="null"/>
        ///   reference.
        /// </exception>
        public static string Sort(string value, CultureInfo culture, CompareOptions options = CompareOptions.None)
        {
            Assert.ArgumentIsNotNull(value, nameof(value));
            Assert.ArgumentIsNotNull(culture, nameof(culture));

            string result = string.Concat(value
                .Distinct()
                .Select(c => new string(c, 1))
                .OrderBy(s => s, culture.CompareInfo.GetStringComparer(options))
            );

            return result;
        }
    }
}
