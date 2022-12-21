
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    internal class Int32KeyArithmeticsTests
    {
        [Test]
        public void SplitAsync_ArgumentRangeIsNull_ThrowsException()
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => Int32KeyArithmetics.Default.SplitAsync(null, CancellationToken.None));
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
        public async Task SplitAsync_ArgumentRangeIsSplittable_SplitsRangeCorrectly(int inputFirst, int inputLast, bool inputInclusive, int returnedFirst, int returnedLast, int createdFirst, int createdLast)
        {
            Range<int> input = new Range<int>(inputFirst, inputLast, inputInclusive);

            RangePartitioningResult<int> result = await Int32KeyArithmetics.Default.SplitAsync(input, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(returnedFirst));
            Assert.That(result.Updated.Last, Is.EqualTo(returnedLast));
            Assert.That(result.Updated.IsInclusive, Is.False);

            Assert.That(result.Success, Is.True);
            Assert.That(result.Created.Count, Is.EqualTo(1));

            Range<int> created = result.Created.First();

            Assert.That(created.First, Is.EqualTo(createdFirst));
            Assert.That(created.Last, Is.EqualTo(createdLast));
            Assert.That(created.IsInclusive, Is.EqualTo(inputInclusive));
        }

        [TestCase(1, 1, true)]
        [TestCase(1, 2, true)]
        [TestCase(1, 3, false)]
        public async Task SplitAsync_ArgumentRangeIsNotSplittable_ReturnUnchangedRange(int inputFirst, int inputLast, bool inputInclusive)
        {
            Range<int> input = new Range<int>(inputFirst, inputLast, inputInclusive);

            RangePartitioningResult<int> result = await Int32KeyArithmetics.Default.SplitAsync(input, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(input.First));
            Assert.That(result.Updated.Last, Is.EqualTo(input.Last));
            Assert.That(result.Updated.IsInclusive, Is.EqualTo(input.IsInclusive));

            Assert.That(result.Success, Is.False);
            Assert.That(result.Created, Is.Empty);
        }
    }
}
