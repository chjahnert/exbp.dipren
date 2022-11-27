
using System.Diagnostics;
using System.Numerics;
using System.Text;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements key arithmetics for the <see cref="string"/> type.
    /// </summary>
    public class StringKeyArithmetics : IKeyArithmetics<string>
    {
        private readonly string _characterset;
        private readonly int _length;
        private readonly BigInteger _combinations;


        /// <summary>
        ///   Initializes a new instance of the <see cref="StringKeyArithmetics"/> class.
        /// </summary>
        /// <param name="characterset">
        /// </param>
        /// <param name="length">
        /// </param>
        public StringKeyArithmetics(string characterset, int length)
        {
            Assert.ArgumentIsNotNull(characterset, nameof(characterset));
            Assert.ArgumentIsNotEmpty(characterset, nameof(characterset));
            Assert.ArgumentIsGreater(length, 0, nameof(length));

            //
            // Compute the number of possible keys.
            //

            BigInteger combinations = 0;

            for (int i = 0; i <= this._length; i++)
            {
                combinations += BigInteger.Pow(this._characterset.Length, i);
            }

            this._characterset = characterset;
            this._length = length;
            this._combinations = combinations;
        }


        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="string"/> to split.
        /// </param>
        /// <param name="created">
        ///   A variable that receives the new <paramref name="range"/> object created.
        /// </param>
        /// <returns>
        ///   A <see cref="Range{TKey}"/> of <see cref="string"/> object that is the updated value of
        ///   <paramref name="range"/>.
        /// </returns>
        public Range<string> Split(Range<string> range, out Range<string> created)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            //
            // TODO: Validate that both keys in the specified range contain only characters that are included in the
            //       current character set.
            //

            Range<string> result = range;
            created = null;

            BigInteger indexFirst = this.ToIndex(range.First);
            BigInteger indexLast = this.ToIndex(range.Last);

            BigInteger distance = BigInteger.Abs(indexLast - indexFirst);

            if (((range.IsInclusive == true) && (distance >= 2)) || ((range.IsInclusive == false) && (distance >= 3)))
            {
                BigInteger half = (distance / 2);

                bool ascending = (indexFirst < indexLast);

                if (ascending == false)
                {
                    half *= -1;
                }

                BigInteger indexMiddle = (indexFirst + half);
                string middle = this.ToKey(indexMiddle);

                result = new Range<string>(range.First, middle, false);
                created = new Range<string>(middle, range.Last, range.IsInclusive);
            }

            return result;
        }

        /// <summary>
        ///   Computes the zero-based index of the specified value in the sorted list of all possible combinations
        ///   of the current character set.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="string"/> value for which to compute the zero-based index.
        /// </param>
        /// <returns>
        ///   the zero-based index of the specified value in the sorted list of all possible combinations
        ///   of the current character set.
        /// </returns>
        private BigInteger ToIndex(string value)
        {
            Debug.Assert(value != null);

            BigInteger combinations = this._combinations;
            BigInteger result = 0;

            for (int i = 0; i < value.Length; i++)
            {
                int index = this._characterset.IndexOf(value[i]);
                combinations /= this._characterset.Length;
                result += ((index * combinations) + 1);
            }

            return result;
        }

        /// <summary>
        ///   Computes the key at the specified index in the sorted list of all possible combinations of the current
        ///   character set.
        /// </summary>
        /// <param name="index">
        ///   The zero-based index of the key to return.
        /// </param>
        /// <returns>
        ///   The key at the specified index.
        /// </returns>
        private string ToKey(BigInteger index)
        {
            StringBuilder builder = new StringBuilder(this._length);

            if (index > 0)
            {
                BigInteger current = 0;
                BigInteger remaining = (this._combinations - 1);

                while (current != index)
                {
                    BigInteger size = (remaining / this._characterset.Length);
                    int i = (int) ((index - current - 1) / size);
                    char character = this._characterset[i];

                    builder.Append(character);

                    current += (size * i) + 1;
                    remaining = size - 1;
                }
            }

            string result = builder.ToString();

            return result;
        }
    }
}
