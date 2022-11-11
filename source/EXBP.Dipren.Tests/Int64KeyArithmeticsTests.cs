
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class Int64KeyArithmeticsTests
    {
        [Test]
        public void Split_ArgumentRangeIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => Int64KeyArithmetics.Default.Split(null, out Range<long> _));
        }

        [TestCase(1, 8, true, 1, 5, 5, 8)]
        [TestCase(8, 1, true, 8, 4, 4, 1)]
        [TestCase(-7, 5, true, -7, -1, -1, 5)]
        [TestCase(5, -7, true, 5, -1, -1, -7)]
        [TestCase(1, 8, false, 1, 5, 5, 8)]
        [TestCase(8, 1, false, 8, 4, 4, 1)]
        [TestCase(-7, 5, false, -7, -1, -1, 5)]
        [TestCase(5, -7, false, 5, -1, -1, -7)]
        [TestCase(long.MinValue, long.MaxValue, false, long.MinValue, 0, 0, long.MaxValue)]
        [TestCase(long.MaxValue, long.MinValue, false, long.MaxValue, -1, -1, long.MinValue)]
        [TestCase(long.MinValue, long.MaxValue, true, long.MinValue, 0, 0, long.MaxValue)]
        [TestCase(long.MaxValue, long.MinValue, true, long.MaxValue, -1, -1, long.MinValue)]
        public void Split_ArgumentRangeIsSplittable_SplitsRangeCorrectly(long inputFirst, long inputLast, bool inputInclusive, long returnedFirst, long returnedLast, long createdFirst, long createdLast)
        {
            Range<long> input = new Range<long>(inputFirst, inputLast, inputInclusive);

            Range<long> returned = Int64KeyArithmetics.Default.Split(input, out Range<long> created);

            Assert.That(returned.First, Is.EqualTo(returnedFirst));
            Assert.That(returned.Last, Is.EqualTo(returnedLast));
            Assert.That(returned.IsInclusive, Is.False);

            Assert.That(created.First, Is.EqualTo(createdFirst));
            Assert.That(created.Last, Is.EqualTo(createdLast));
            Assert.That(created.IsInclusive, Is.EqualTo(inputInclusive));
        }

        [TestCase(1, 1, true)]
        [TestCase(1, 2, true)]
        [TestCase(1, 3, false)]
        public void Split_ArgumentRangeIsNotSplittable_ReturnUnchangedRange(long inputFirst, long inputLast, bool inputInclusive)
        {
            Range<long> input = new Range<long>(inputFirst, inputLast, inputInclusive);

            Range<long> returned = Int64KeyArithmetics.Default.Split(input, out Range<long> created);

            Assert.That(returned.First, Is.EqualTo(input.First));
            Assert.That(returned.Last, Is.EqualTo(input.Last));
            Assert.That(returned.IsInclusive, Is.EqualTo(input.IsInclusive));

            Assert.That(created, Is.Null);
        }
    }
}
