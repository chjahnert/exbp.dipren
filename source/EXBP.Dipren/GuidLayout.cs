
namespace EXBP.Dipren
{
    /// <summary>
    ///   Defines well known GUID/UUID layouts in little endian order.
    /// </summary>
    public static class GuidLayout
    {
        private const int GUID_LENGTH = 16;


        /// <summary>
        ///   Gets byte layout of an GUID value used by Microsoft SQL Server.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values in little endian order
        ///   used by Microsoft SQL Server.
        /// </value>
        public static byte[] MicrosoftSqlServer { get; } = { 10, 11, 12, 13, 14, 15, 8, 9, 6, 7, 4, 5, 0, 1, 2, 3 };

        /// <summary>
        ///   Gets byte layout of an GUID value in lexicographical order.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values in lexicographical order.
        /// </value>
        public static byte[] LexicographicalOrder { get; } = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

        /// <summary>
        ///   Gets byte layout of an GUID value used by .NET Framework.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values in little endian order
        ///   used by the .NET Framework.
        /// </value>
        public static byte[] DoNetFramework { get; } = { 3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15 };


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
                    if (ordered[i] != 0)
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
