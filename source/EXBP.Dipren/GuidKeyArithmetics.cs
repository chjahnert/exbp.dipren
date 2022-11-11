
using System.Diagnostics;
using System.Numerics;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements key arithmetics for the <see cref="Guid"/> type.
    /// </summary>
    public class GuidKeyArithmetics : IKeyArithmetics<Guid>
    {
        private const int GUID_LENGTH = 16;

        private readonly byte[] _layout;


        /// <summary>
        ///   Gets a <see cref="GuidKeyArithmetics"/> object that uses the GUID layout used by Microsoft SQL Server.
        /// </summary>
        /// <value>
        ///   A <see cref="GuidKeyArithmetics"/> object that uses the GUID layout used by Microsoft SQL Server.
        /// </value>
        public static GuidKeyArithmetics MicrosoftSqlServer { get; } = new GuidKeyArithmetics(GuidLayout.MicrosoftSqlServer);

        /// <summary>
        ///   Gets a <see cref="GuidKeyArithmetics"/> object that uses a bytewise lexicographical GUID layout.
        /// </summary>
        /// <value>
        ///   A <see cref="GuidKeyArithmetics"/> object that uses a bytewise lexicographical GUID layout.
        /// </value>
        public static GuidKeyArithmetics LexicographicalBytewise { get; } = new GuidKeyArithmetics(GuidLayout.LexicographicalBytewise);

        /// <summary>
        ///   Gets a <see cref="GuidKeyArithmetics"/> object that uses a memberwise lexicographical GUID layout.
        /// </summary>
        /// <value>
        ///   A <see cref="GuidKeyArithmetics"/> object that uses a memberwise lexicographical GUID layout.
        /// </value>
        public static GuidKeyArithmetics LexicographicalMemberwise { get; } = new GuidKeyArithmetics(GuidLayout.LexicographicalMemberwise);


        /// <summary>
        ///   Initializes a new instance of the <see cref="GuidKeyArithmetics"/> class.
        /// </summary>
        /// <param name="layout">
        ///   An array of 16 <see cref="byte"/> values between 0 and 15 indicating the layout of UUID values in little
        ///   endian order.
        /// </param>
        public GuidKeyArithmetics(byte[] layout)
        {
            Assert.ArgumentIsNotNull(layout, nameof(layout));

            bool valid = GuidLayout.IsValid(layout);

            Assert.ArgumentIsValid(valid, nameof(layout), GuidKeyArithmeticsResources.MessageInvalidGuidLayout);

            this._layout = layout;
        }


        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="Guid"/> values to split.
        /// </param>
        /// <param name="created">
        ///   A variable that receives the new <see cref="Range{TKey}"/> of <see cref="Guid"/> object.
        /// </param>
        /// <returns>
        ///   A <see cref="Range{TKey}"/> of <see cref="Guid"/> object that is the updated <paramref name="range"/>.
        /// </returns>
        public Range<Guid> Split(Range<Guid> range, out Range<Guid> created)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            created = null;

            Range<BigInteger> rangeBi = this.ToBigIntegerRange(range);
            Range<BigInteger> resultBi = BigIntegerKeyArithmetics.Default.Split(rangeBi, out Range<BigInteger> createdBi);

            Range<Guid> result = this.ToGuidRange(resultBi);

            if (createdBi != null)
            {
                created = this.ToGuidRange(createdBi);
            }

            return result;
        }

        /// <summary>
        ///   Converts the specified <see cref="Range{TKey}"/> of <see cref="Guid"/> object to a
        ///   <see cref="Range{TKey}"/> of <see cref="BigInteger"/> object.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="Range{TKey}"/> of <see cref="Guid"/> object to convert.
        /// </param>
        /// <returns>
        ///   The <see cref="Range{TKey}"/> of <see cref="BigInteger"/> object converted from <paramref name="value"/>.
        /// </returns>
        private Range<BigInteger> ToBigIntegerRange(Range<Guid> value)
        {
            Debug.Assert(value != null);

            BigInteger first = this.ToBigInteger(value.First);
            BigInteger last = this.ToBigInteger(value.Last);

            Range<BigInteger> result = new Range<BigInteger>(first, last, value.IsInclusive);

            return result;
        }

        /// <summary>
        ///   Converts the specified <see cref="Range{TKey}"/> of <see cref="BigInteger"/> object to a
        ///   <see cref="Range{TKey}"/> of <see cref="Guid"/> object.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="Range{TKey}"/> of <see cref="BigInteger"/> object to convert.
        /// </param>
        /// <returns>
        ///   The <see cref="Range{TKey}"/> of <see cref="Guid"/> object converted from <paramref name="value"/>.
        /// </returns>
        private Range<Guid> ToGuidRange(Range<BigInteger> value)
        {
            Debug.Assert(value != null);

            Guid first = this.ToGuid(value.First);
            Guid last = this.ToGuid(value.Last);

            Range<Guid> result = new Range<Guid>(first, last, value.IsInclusive);

            return result;
        }

        /// <summary>
        ///   Converts the specified <see cref="Guid"/> value to a <see cref="BigInteger"/> value.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="Guid"/> value to convert.
        /// </param>
        /// <returns>
        ///   The <see cref="BigInteger"/> value that is the result of the conversion.
        /// </returns>
        private BigInteger ToBigInteger(Guid value)
        {
            byte[] source = value.ToByteArray();
            byte[] target = new byte[GUID_LENGTH + 1];

            for (int i = 0; i < GUID_LENGTH; i++)
            {
                target[GUID_LENGTH - 1 - i] = source[this._layout[i]];
            }

            BigInteger result = new BigInteger(target);

            return result;
        }

        /// <summary>
        ///   Converts the specified <see cref="BigInteger"/> value to a <see cref="Guid"/> value.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="BigInteger"/> value to convert.
        /// </param>
        /// <returns>
        ///   The <see cref="Guid"/> value that is the result of the conversion.
        /// </returns>
        private Guid ToGuid(BigInteger value)
        {
            byte[] bytes = value.ToByteArray();
            byte[] source = new byte[GUID_LENGTH];

            Array.Copy(bytes, 0, source, 0, (bytes.Length > GUID_LENGTH ? GUID_LENGTH : bytes.Length));

            byte[] target = new byte[GUID_LENGTH];

            for (int i = 0; i < GUID_LENGTH; i++)
            {
                target[this._layout[i]] = source[GUID_LENGTH - 1 - i];
            }

            return new Guid(target);
        }
    }
}
