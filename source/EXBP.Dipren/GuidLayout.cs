
namespace EXBP.Dipren
{
    /// <summary>
    ///   Defines well known GUID/UUID layouts.
    /// </summary>
    public static class GuidLayout
    {
        private const int GUID_LENGTH = 16;


        /// <summary>
        ///   Gets byte layout of an GUID value used by Microsoft SQL Server.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values used by Microsoft SQL
        ///   Server. The positions are specified from the least significant on the left to the most significant on the
        ///   right.
        /// </value>
        public static byte[] MicrosoftSqlServer { get; } = { 10, 11, 12, 13, 14, 15, 8, 9, 6, 7, 4, 5, 0, 1, 2, 3 };

        /// <summary>
        ///   Gets byte layout of an GUID value in lexicographical order.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values in lexicographical order.
        ///   The positions are specified from the least significant on the left to the most significant on the right.
        /// </value>
        public static byte[] LexicographicalOrder { get; } = { 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };

        /// <summary>
        ///   Gets byte layout of an GUID value used by .NET Framework.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values used by the .NET
        ///   Framework. The positions are specified from the least significant on the left to the most significant on
        ///   the right.
        /// </value>
        public static byte[] DotNetFramework { get; } = { 3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15 };


        /// <summary>
        ///   Determines whether the specified <see cref="byte"/> array is a valid GUID layout.
        /// </summary>
        /// <param name="layout">
        ///   An array of 16 <see cref="byte"/> values between 0 and 15.
        /// </param>
        /// <returns>
        ///   <see langword="true"/> if <see cref="layout"/> is valid; otherwise, <see langword="false"/>.
        /// </returns>
        internal static bool IsValid(byte[] layout)
        {
            bool result = false;

            if (layout?.Length == GUID_LENGTH)
            {
                result = true;

                List<byte> ordered = layout.OrderBy(x => x).ToList();

                for (int i = 0; i < GUID_LENGTH; i++)
                {
                    if (ordered[i] != i)
                    {
                        result = false;
                        break;
                    }
                }
            }

            return result;
        }
    }
}
