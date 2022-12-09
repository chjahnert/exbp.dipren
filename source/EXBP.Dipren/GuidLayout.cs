
namespace EXBP.Dipren
{
    /// <summary>
    ///   Defines well known GUID/UUID layouts.
    /// </summary>
    public static class GuidLayout
    {
        private const int GUID_LENGTH = 16;


        /// <summary>
        ///   Gets byte layout of a GUID value used by Microsoft SQL Server.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values used by Microsoft SQL
        ///   Server.
        /// </value>
        public static byte[] MicrosoftSqlServer { get; } = { 10, 11, 12, 13, 14, 15, 8, 9, 6, 7, 4, 5, 0, 1, 2, 3 };

        /// <summary>
        ///   Gets the layout of a GUID value in byte-wise lexicographical order.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values in byte-wise
        ///   lexicographical order.
        /// </value>
        /// <remarks>
        ///   This is the layout used by <c>memcmp</c>.
        /// </remarks>
        public static byte[] LexicographicalBytewise { get; } = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

        /// <summary>
        ///   Gets the layout of a GUID value in member-wise lexicographical order.
        /// </summary>
        /// <value>
        ///   An array of 16 <see cref="byte"/> values containing the layout for GUID values in member-wise
        ///   lexicographical order.
        /// </value>
        /// <remarks>
        ///   The layout is used by the <see cref="Guid"/> type and by Postgres SQL Server.
        /// </remarks>
        public static byte[] LexicographicalMemberwise { get; } = { 3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15 };


        /// <summary>
        ///   Determines whether the specified <see cref="byte"/> array is a valid GUID layout.
        /// </summary>
        /// <param name="layout">
        ///   An array of 16 <see cref="byte"/> values between 0 and 15.
        /// </param>
        /// <returns>
        ///   <see langword="true"/> if <paramref name="layout"/> is valid; otherwise, <see langword="false"/>.
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
