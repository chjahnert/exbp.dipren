
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class StringKeyArithmeticsTests
    {
        [Test]
        public void Ctor_ArgumentCharactersetIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new StringKeyArithmetics(null, 3));
        }

        [Test]
        public void Ctor_ArgumentCharactersetIsEmpty_ThrowsException()
        {
            Assert.Throws<ArgumentException>(() => new StringKeyArithmetics(String.Empty, 3));
        }

        [Test]
        public void Ctor_ArgumentCharactersetContainsDuplicates_ThrowsException()
        {
            Assert.Throws<ArgumentException>(() => new StringKeyArithmetics("butter", 3));
        }

        [TestCase(-1)]
        [TestCase(0)]
        public void Ctor_ArgumentLengthIsLessThanOne_ThrowsException(int length)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new StringKeyArithmetics("abc", length));
        }

        [Test]
        public void Split_ArgumentRangeIsNull_ThrowsException()
        {
            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 1);

            Assert.Throws<ArgumentNullException>(() => arithmetics.Split(null, out _));
        }

        [Test]
        public void Split_FirstKeyInRangeIsTooLong_ThrowsException()
        {
            Range<string> range = new Range<string>("aba", "bb", false);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 2);

            Assert.Throws<ArgumentException>(() => arithmetics.Split(range, out _), StringKeyArithmeticsResources.MessageFirstKeyInRangeTooLong);
        }

        [Test]
        public void Split_FirstKeyInRangeContainsInvalidCharacters_ThrowsException()
        {
            Range<string> range = new Range<string>("az", "bb", false);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 2);

            Assert.Throws<ArgumentException>(() => arithmetics.Split(range, out _), StringKeyArithmeticsResources.MessageFirstKeyInRangeContainsInvalidCharacters);
        }

        [Test]
        public void Split_LastKeyInRangeIsTooLong_ThrowsException()
        {
            Range<string> range = new Range<string>("ab", "bba", false);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 2);

            Assert.Throws<ArgumentException>(() => arithmetics.Split(range, out _), StringKeyArithmeticsResources.MessageLastKeyInRangeTooLong);
        }

        [Test]
        public void Split_LasstKeyInRangeContainsInvalidCharacters_ThrowsException()
        {
            Range<string> range = new Range<string>("aa", "bz", false);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 2);

            Assert.Throws<ArgumentException>(() => arithmetics.Split(range, out _), StringKeyArithmeticsResources.MessageLastKeyInRangeContainsInvalidCharacters);
        }

        [TestCase("0123456789", 3, "333", "335", false)]
        [TestCase("1ab", 3, "1a", "1a1", true)]
        public void Split_RangeIsTooSmall_DoesNotSplitsRange(string characterset, int length, string first, string last, bool inclusive)
        {
            Range<string> whole = new Range<string>(first, last, inclusive);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics(characterset, length);

            Range<string> updated = arithmetics.Split(whole, out Range<string> created);

            Assert.That(updated, Is.Not.Null);
            Assert.That(updated.First, Is.EqualTo(first));
            Assert.That(updated.Last, Is.EqualTo(last));
            Assert.That(updated.IsInclusive, Is.EqualTo(inclusive));

            Assert.That(created, Is.Null);
        }

        [TestCase("0123456789", 3, "3", "7", true, "5")]
        [TestCase("1ab", 3, "a1", "aba", true, "aa1")]
        [TestCase("1ab", 3, "", "bbb", true, "aa")]
        public void Split_RangeIsLargeEnough_SplitsRange(string characterset, int length, string first, string last, bool inclusive, string expected)
        {
            Range<string> whole = new Range<string>(first, last, inclusive);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics(characterset, length);

            Range<string> updated = arithmetics.Split(whole, out Range<string> created);

            Assert.That(updated, Is.Not.Null);
            Assert.That(updated.First, Is.EqualTo(first));
            Assert.That(updated.Last, Is.EqualTo(expected));
            Assert.That(updated.IsInclusive, Is.False);

            Assert.That(created, Is.Not.Null);
            Assert.That(created.First, Is.EqualTo(expected));
            Assert.That(created.Last, Is.EqualTo(last));
            Assert.That(created.IsInclusive, Is.EqualTo(inclusive));
        }

        [Test]
        public void SplitAsync_ArgumentRangeIsNull_ThrowsException()
        {
            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 1);

            Assert.ThrowsAsync<ArgumentNullException>(() => arithmetics.SplitAsync(null, CancellationToken.None));
        }

        [Test]
        public void SplitAsync_FirstKeyInRangeIsTooLong_ThrowsException()
        {
            Range<string> range = new Range<string>("aba", "bb", false);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 2);

            Assert.ThrowsAsync<ArgumentException>(() => arithmetics.SplitAsync(range, CancellationToken.None), StringKeyArithmeticsResources.MessageFirstKeyInRangeTooLong);
        }

        [Test]
        public void SplitAsync_FirstKeyInRangeContainsInvalidCharacters_ThrowsException()
        {
            Range<string> range = new Range<string>("az", "bb", false);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 2);

            Assert.ThrowsAsync<ArgumentException>(() => arithmetics.SplitAsync(range, CancellationToken.None), StringKeyArithmeticsResources.MessageFirstKeyInRangeContainsInvalidCharacters);
        }

        [Test]
        public void SplitAsync_LastKeyInRangeIsTooLong_ThrowsException()
        {
            Range<string> range = new Range<string>("ab", "bba", false);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 2);

            Assert.ThrowsAsync<ArgumentException>(() => arithmetics.SplitAsync(range, CancellationToken.None), StringKeyArithmeticsResources.MessageLastKeyInRangeTooLong);
        }

        [Test]
        public void SplitAsync_LasstKeyInRangeContainsInvalidCharacters_ThrowsException()
        {
            Range<string> range = new Range<string>("aa", "bz", false);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics("abc", 2);

            Assert.ThrowsAsync<ArgumentException>(() => arithmetics.SplitAsync(range, CancellationToken.None), StringKeyArithmeticsResources.MessageLastKeyInRangeContainsInvalidCharacters);
        }

        [TestCase("0123456789", 3, "333", "335", false)]
        [TestCase("1ab", 3, "1a", "1a1", true)]
        public async Task SplitAsync_RangeIsTooSmall_DoesNotSplitsRange(string characterset, int length, string first, string last, bool inclusive)
        {
            Range<string> whole = new Range<string>(first, last, inclusive);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics(characterset, length);

            RangePartitioningResult<string> result = await arithmetics.SplitAsync(whole, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(first));
            Assert.That(result.Updated.Last, Is.EqualTo(last));
            Assert.That(result.Updated.IsInclusive, Is.EqualTo(inclusive));

            Assert.That(result.Success, Is.False);
            Assert.That(result.Created, Is.Empty);
        }

        [TestCase("0123456789", 3, "3", "7", true, "5")]
        [TestCase("1ab", 3, "a1", "aba", true, "aa1")]
        [TestCase("1ab", 3, "", "bbb", true, "aa")]
        public async Task SplitAsync_RangeIsLargeEnough_SplitsRange(string characterset, int length, string first, string last, bool inclusive, string expected)
        {
            Range<string> whole = new Range<string>(first, last, inclusive);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics(characterset, length);

            RangePartitioningResult<string> result = await arithmetics.SplitAsync(whole, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(first));
            Assert.That(result.Updated.Last, Is.EqualTo(expected));
            Assert.That(result.Updated.IsInclusive, Is.False);

            Assert.That(result.Success, Is.True);
            Assert.That(result.Created.Count, Is.EqualTo(1));

            Range<string> created = result.Created.First();

            Assert.That(created, Is.Not.Null);
            Assert.That(created.First, Is.EqualTo(expected));
            Assert.That(created.Last, Is.EqualTo(last));
            Assert.That(created.IsInclusive, Is.EqualTo(inclusive));
        }
    }
}
