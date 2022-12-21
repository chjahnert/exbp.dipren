
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
        private readonly string _characters;
        private readonly int _length;
        private readonly BigInteger _combinations;


        /// <summary>
        ///   Gets a <see cref="string"/> containing all possible characters allowed in the key, ordered according to
        ///   the sorting rules used by the underlaying database.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value containing all possible characters allowed in the key, ordered according to
        ///   the sorting rules used by the underlaying database.
        /// </value>
        protected string Characters => this._characters;

        /// <summary>
        ///   Gets the maximum length of a key, expressed in number of characters.
        /// </summary>
        /// <value>
        ///   An <see cref="int"/> value that contains the maximum number of characters a key may contain.
        /// </value>
        protected int Length => this._length;


        /// <summary>
        ///   Initializes a new instance of the <see cref="StringKeyArithmetics"/> class.
        /// </summary>
        /// <param name="characters">
        ///   A <see cref="string"/> containing all possible characters allowed in the key sorted according to the
        ///   sorting rules used by the underlaying data source.
        /// </param>
        /// <param name="length">
        ///   The maximum length of the key.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="characters"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///   Argument <paramref name="characters"/> contains duplicate characters.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///   Argument <paramref name="length"/> is less than one.
        /// </exception>
        /// <remarks>
        ///   <para>
        ///     When specifying the <paramref name="characters"/>, the order in which the characters appear is
        ///     important. They should be ordered the same way as the underlaying database would sort them. If the
        ///     character set contained 'a' and 'b', the possible key combinations in ascending order would be:
        ///     '', 'a', 'aa', 'ab', 'b', 'ba', and 'bb'. In contrast, if character set contained 'b' and 'a' instead,
        ///     the possible key combinations in ascending order would be: '', 'b', 'bb', 'ba', 'a', 'ab', and 'aa'.
        ///   </para>
        /// </remarks>
        public StringKeyArithmetics(string characters, int length)
        {
            Assert.ArgumentIsNotNull(characters, nameof(characters));
            Assert.ArgumentIsNotEmpty(characters, nameof(characters));
            Assert.ArgumentIsValid(characters.Length == characters.Distinct().Count(), nameof(characters), StringKeyArithmeticsResources.MessageCharactersHaveToBeUnique);
            Assert.ArgumentIsGreater(length, 0, nameof(length));

            BigInteger combinations = 0;

            for (int i = 0; i <= length; i++)
            {
                combinations += BigInteger.Pow(characters.Length, i);
            }

            this._characters = characters;
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
        public virtual Range<string> Split(Range<string> range, out Range<string> created)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));
            Assert.ArgumentIsValid(range.First.Length <= this._length, nameof(range), StringKeyArithmeticsResources.MessageFirstKeyInRangeTooLong);
            Assert.ArgumentIsValid(range.First.All(c => this._characters.Contains(c)), nameof(range), StringKeyArithmeticsResources.MessageFirstKeyInRangeContainsInvalidCharacters);
            Assert.ArgumentIsValid(range.Last.Length <= this._length, nameof(range), StringKeyArithmeticsResources.MessageLastKeyInRangeTooLong);
            Assert.ArgumentIsValid(range.Last.All(c => this._characters.Contains(c)), nameof(range), StringKeyArithmeticsResources.MessageLastKeyInRangeContainsInvalidCharacters);

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
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="string"/> to split.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> object that represents the asynchronous operation.
        /// </returns>
        public virtual async Task<RangePartitioningResult<string>> SplitAsync(Range<string> range, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));
            Assert.ArgumentIsValid(range.First.Length <= this._length, nameof(range), StringKeyArithmeticsResources.MessageFirstKeyInRangeTooLong);
            Assert.ArgumentIsValid(range.First.All(c => this._characters.Contains(c)), nameof(range), StringKeyArithmeticsResources.MessageFirstKeyInRangeContainsInvalidCharacters);
            Assert.ArgumentIsValid(range.Last.Length <= this._length, nameof(range), StringKeyArithmeticsResources.MessageLastKeyInRangeTooLong);
            Assert.ArgumentIsValid(range.Last.All(c => this._characters.Contains(c)), nameof(range), StringKeyArithmeticsResources.MessageLastKeyInRangeContainsInvalidCharacters);

            Range<BigInteger> rangeBi = this.ToBigIntegerRange(range);
            RangePartitioningResult<BigInteger> resultBi = await BigIntegerKeyArithmetics.Default.SplitAsync(rangeBi, cancellation);

            RangePartitioningResult<string> result;

            if (resultBi?.Success == true)
            {
                Range<string> updated = this.ToStringRange(resultBi.Updated);
                IEnumerable<Range<string>> created = resultBi.Created.Select(r => this.ToStringRange(r));

                result = new RangePartitioningResult<string>(updated, created);
            }
            else
            {
                result = new RangePartitioningResult<string>(range, new Range<string>[0]);
            }

            return result;
        }

        /// <summary>
        ///   Converts the specified <see cref="string"/> key range to a <see cref="BigInteger"/> key range.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="string"/> object to convert.
        /// </param>
        /// <returns>
        ///   The <see cref="Range{TKey}"/> of <see cref="BigInteger"/> that is the result of the conversion.
        /// </returns>
        private Range<BigInteger> ToBigIntegerRange(Range<string> range)
        {
            Debug.Assert(range != null);

            BigInteger first = this.ToIndex(range.First);
            BigInteger last = this.ToIndex(range.Last);

            Range<BigInteger> result = new Range<BigInteger>(first, last, range.IsInclusive);

            return result;
        }

        /// <summary>
        ///   Converts the specified <see cref="BigInteger"/> key range to a <see cref="string"/> key range.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="BigInteger"/> object to convert.
        /// </param>
        /// <returns>
        ///   The <see cref="Range{TKey}"/> of <see cref="string"/> that is the result of the conversion.
        /// </returns>
        private Range<string> ToStringRange(Range<BigInteger> range)
        {
            Debug.Assert(range != null);

            string first = this.ToKey(range.First);
            string last = this.ToKey(range.Last);

            Range<string> result = new Range<string>(first, last, range.IsInclusive);

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
                int index = this._characters.IndexOf(value[i]);
                combinations /= this._characters.Length;
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
                    BigInteger size = (remaining / this._characters.Length);
                    int i = (int) ((index - current - 1) / size);
                    char character = this._characters[i];

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
