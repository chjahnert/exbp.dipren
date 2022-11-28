
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
        ///   A <see cref="string"/> containing all possible characters allowed in the key sorted according to the
        ///   sorting rules used by the underlaying database.
        /// </param>
        /// <param name="length">
        ///   The maximum length of the key.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="characterset"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///   Argument <paramref name="characterset"/> contains duplicate characters.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///   Argument <paramref name="length"/> is less than one.
        /// </exception>
        /// <remarks>
        ///   <para>
        ///     When specifying the <paramref name="characterset"/>, the order in which the characters appear is
        ///     important. They should be ordered the same way as the underlaying database would sort them. If the
        ///     character set contained 'a' and 'b', the possible key combinations in ascending order would be:
        ///     '', 'a', 'aa', 'ab', 'b', 'ba', and 'bb'. In contrast, if character set contained 'b' and 'a' instead,
        ///     the possible key combinations in ascending order would be: '', 'b', 'bb', 'ba', 'a', 'ab', and 'aa'.
        ///   </para>
        /// </remarks>
        public StringKeyArithmetics(string characterset, int length)
        {
            Assert.ArgumentIsNotNull(characterset, nameof(characterset));
            Assert.ArgumentIsNotEmpty(characterset, nameof(characterset));
            Assert.ArgumentIsValid(characterset.Length == characterset.Distinct().Count(), nameof(characterset), StringKeyArithmeticsResources.MessageCharactersHaveToBeUnique);
            Assert.ArgumentIsGreater(length, 0, nameof(length));

            BigInteger combinations = 0;

            for (int i = 0; i <= length; i++)
            {
                combinations += BigInteger.Pow(characterset.Length, i);
            }

            this._characterset = characterset;
            this._combinations = combinations;
            this._length = length;
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
            Assert.ArgumentIsValid(range.First.Length <= this._length, nameof(range), StringKeyArithmeticsResources.MessageFirstKeyInRangeTooLong);
            Assert.ArgumentIsValid(range.First.All(c => this._characterset.Contains(c)), nameof(range), StringKeyArithmeticsResources.MessageFirstKeyInRangeContainsInvalidCharacters);
            Assert.ArgumentIsValid(range.Last.Length <= this._length, nameof(range), StringKeyArithmeticsResources.MessageLastKeyInRangeTooLong);
            Assert.ArgumentIsValid(range.Last.All(c => this._characterset.Contains(c)), nameof(range), StringKeyArithmeticsResources.MessageLastKeyInRangeContainsInvalidCharacters);

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
