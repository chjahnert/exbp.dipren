
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class StringKeyRangePartitionerTests
    {
        [Test]
        public void Ctor_ArgumentCharactersetIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new StringKeyRangePartitioner(null, 3));
        }

        [Test]
        public void Ctor_ArgumentCharactersetIsEmpty_ThrowsException()
        {
            Assert.Throws<ArgumentException>(() => new StringKeyRangePartitioner(String.Empty, 3));
        }

        [Test]
        public void Ctor_ArgumentCharactersetContainsDuplicates_ThrowsException()
        {
            Assert.Throws<ArgumentException>(() => new StringKeyRangePartitioner("butter", 3));
        }

        [TestCase(-1)]
        [TestCase(0)]
        public void Ctor_ArgumentLengthIsLessThanOne_ThrowsException(int length)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new StringKeyRangePartitioner("abc", length));
        }

        [Test]
        public void SplitAsync_ArgumentRangeIsNull_ThrowsException()
        {
            StringKeyRangePartitioner partitioner = new StringKeyRangePartitioner("abc", 1);

            Assert.ThrowsAsync<ArgumentNullException>(() => partitioner.SplitAsync(null, CancellationToken.None));
        }

        [Test]
        public void SplitAsync_FirstKeyInRangeIsTooLong_ThrowsException()
        {
            Range<string> range = new Range<string>("aba", "bb", false);

            StringKeyRangePartitioner partitioner = new StringKeyRangePartitioner("abc", 2);

            Assert.ThrowsAsync<ArgumentException>(() => partitioner.SplitAsync(range, CancellationToken.None), StringKeyRangePartitionerResources.MessageFirstKeyInRangeTooLong);
        }

        [Test]
        public void SplitAsync_FirstKeyInRangeContainsInvalidCharacters_ThrowsException()
        {
            Range<string> range = new Range<string>("az", "bb", false);

            StringKeyRangePartitioner partitioner = new StringKeyRangePartitioner("abc", 2);

            Assert.ThrowsAsync<ArgumentException>(() => partitioner.SplitAsync(range, CancellationToken.None), StringKeyRangePartitionerResources.MessageFirstKeyInRangeContainsInvalidCharacters);
        }

        [Test]
        public void SplitAsync_LastKeyInRangeIsTooLong_ThrowsException()
        {
            Range<string> range = new Range<string>("ab", "bba", false);

            StringKeyRangePartitioner partitioner = new StringKeyRangePartitioner("abc", 2);

            Assert.ThrowsAsync<ArgumentException>(() => partitioner.SplitAsync(range, CancellationToken.None), StringKeyRangePartitionerResources.MessageLastKeyInRangeTooLong);
        }

        [Test]
        public void SplitAsync_LasstKeyInRangeContainsInvalidCharacters_ThrowsException()
        {
            Range<string> range = new Range<string>("aa", "bz", false);

            StringKeyRangePartitioner partitioner = new StringKeyRangePartitioner("abc", 2);

            Assert.ThrowsAsync<ArgumentException>(() => partitioner.SplitAsync(range, CancellationToken.None), StringKeyRangePartitionerResources.MessageLastKeyInRangeContainsInvalidCharacters);
        }

        [TestCase("0123456789", 3, "333", "335", false)]
        [TestCase("1ab", 3, "1a", "1a1", true)]
        public async Task SplitAsync_RangeIsTooSmall_DoesNotSplitsRange(string characterset, int length, string first, string last, bool inclusive)
        {
            Range<string> whole = new Range<string>(first, last, inclusive);

            StringKeyRangePartitioner partitioner = new StringKeyRangePartitioner(characterset, length);

            RangePartitioningResult<string> result = await partitioner.SplitAsync(whole, CancellationToken.None);

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

            StringKeyRangePartitioner partitioner = new StringKeyRangePartitioner(characterset, length);

            RangePartitioningResult<string> result = await partitioner.SplitAsync(whole, CancellationToken.None);

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
