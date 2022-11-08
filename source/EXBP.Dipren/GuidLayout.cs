
namespace EXBP.Dipren
{
    /// <summary>
    ///   Defines well known GUID/UUID layouts in little endian order.
    /// </summary>
    public static class GuidLayout
    {
        private const int GUID_LENGTH = 16;


        /// <summary>
        ///   Gets byte layout of an UUID value used by Microsoft SQL Server.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values in little endian order
        ///   used by Microsoft SQL Server.
        /// </value>
        public static byte[] MicrosoftSqlServer { get; } = { 10, 11, 12, 13, 14, 15, 8, 9, 6, 7, 4, 5, 0, 1, 2, 3 };


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
