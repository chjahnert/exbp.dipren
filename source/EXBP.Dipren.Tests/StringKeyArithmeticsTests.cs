
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
    }
}
