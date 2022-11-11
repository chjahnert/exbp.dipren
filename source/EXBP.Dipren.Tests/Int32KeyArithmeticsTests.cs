
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    internal class Int32KeyArithmeticsTests
    {
        [Test]
        public void Split_ArgumentRangeIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => Int32KeyArithmetics.Default.Split(null, out Range<int> _));
        }

        [TestCase(1, 8, true, 1, 5, 5, 8)]
        [TestCase(8, 1, true, 8, 4, 4, 1)]
        [TestCase(-7, 5, true, -7, -1, -1, 5)]
        [TestCase(5, -7, true, 5, -1, -1, -7)]
        [TestCase(1, 8, false, 1, 5, 5, 8)]
        [TestCase(8, 1, false, 8, 4, 4, 1)]
        [TestCase(-7, 5, false, -7, -1, -1, 5)]
        [TestCase(5, -7, false, 5, -1, -1, -7)]
        [TestCase(int.MinValue, int.MaxValue, false, int.MinValue, 0, 0, int.MaxValue)]
        [TestCase(int.MaxValue, int.MinValue, false, int.MaxValue, -1, -1, int.MinValue)]
        [TestCase(int.MinValue, int.MaxValue, true, int.MinValue, 0, 0, int.MaxValue)]
        [TestCase(int.MaxValue, int.MinValue, true, int.MaxValue, -1, -1, int.MinValue)]
        public void Split_ArgumentRangeIsSplittable_SplitsRangeCorrectly(int inputFirst, int inputLast, bool inputInclusive, int returnedFirst, int returnedLast, int createdFirst, int createdLast)
        {
            Range<int> input = new Range<int>(inputFirst, inputLast, inputInclusive);

            Range<int> returned = Int32KeyArithmetics.Default.Split(input, out Range<int> created);

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
        public void Split_ArgumentRangeIsNotSplittable_ReturnUnchangedRange(int inputFirst, int inputLast, bool inputInclusive)
        {
            Range<int> input = new Range<int>(inputFirst, inputLast, inputInclusive);

            Range<int> returned = Int32KeyArithmetics.Default.Split(input, out Range<int> created);

            Assert.That(returned.First, Is.EqualTo(input.First));
            Assert.That(returned.Last, Is.EqualTo(input.Last));
            Assert.That(returned.IsInclusive, Is.EqualTo(input.IsInclusive));

            Assert.That(created, Is.Null);
        }
    }
}
