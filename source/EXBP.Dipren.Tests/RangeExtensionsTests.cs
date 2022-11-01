
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class RangeExtensionsTests
    {
        [Test]
        public void IsAscending_ArgumentRangeIsNull_ThrowsException()
        {
            Range<int> range = null;

            Assert.Throws<ArgumentNullException>(() => range.IsAscending(Comparer<int>.Default));
        }

        [Test]
        public void IsAscending_ArgumentComparerIsNull_ThrowsException()
        {
            Range<int> range = new Range<int>(1, 3, true);

            Assert.Throws<ArgumentNullException>(() => range.IsAscending(null));
        }

        [TestCase(1, 3, true, true)]
        [TestCase(-1, 1, true, true)]
        [TestCase(-7, -6, true, true)]
        [TestCase(1, 3, false, true)]
        [TestCase(-1, 1, false, true)]
        [TestCase(-7, -6, false, true)]
        [TestCase(2, 2, false, true)]
        [TestCase(0, 0, false, true)]
        [TestCase(-2, -2, false, true)]
        [TestCase(3, 1, true, false)]
        [TestCase(1, -1, true, false)]
        [TestCase(-1, -3, true, false)]
        [TestCase(3, 1, false, false)]
        [TestCase(1, -1, false, false)]
        [TestCase(-1, -3, false, false)]
        public void IsAscending_ArgumentComparerIsNull_ThrowsException(int first, int last, bool inclusive, bool expected)
        {
            Range<int> range = new Range<int>(first, last, inclusive);

            bool result = range.IsAscending(Comparer<int>.Default);

            Assert.That( result, Is.EqualTo(expected));
        }
    }
}
