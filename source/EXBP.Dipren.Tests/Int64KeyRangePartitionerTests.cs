﻿
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class Int64KeyRangePartitionerTests
    {
        [Test]
        public void SplitAsync_ArgumentRangeIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => Int64KeyRangePartitioner.Default.SplitAsync(null, CancellationToken.None));
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
        public async Task SplitAsync_ArgumentRangeIsSplittable_SplitsRangeCorrectly(long inputFirst, long inputLast, bool inputInclusive, long returnedFirst, long returnedLast, long createdFirst, long createdLast)
        {
            Range<long> input = new Range<long>(inputFirst, inputLast, inputInclusive);

            RangePartitioningResult<long> result = await Int64KeyRangePartitioner.Default.SplitAsync(input, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(returnedFirst));
            Assert.That(result.Updated.Last, Is.EqualTo(returnedLast));
            Assert.That(result.Updated.IsInclusive, Is.False);

            Assert.That(result.Success, Is.True);
            Assert.That(result.Created.Count, Is.EqualTo(1));

            Range<long> created = result.Created.First();

            Assert.That(created.First, Is.EqualTo(createdFirst));
            Assert.That(created.Last, Is.EqualTo(createdLast));
            Assert.That(created.IsInclusive, Is.EqualTo(inputInclusive));
        }

        [TestCase(1, 1, true)]
        [TestCase(1, 2, true)]
        [TestCase(1, 3, false)]
        public async Task SplitAsync_ArgumentRangeIsNotSplittable_ReturnUnchangedRange(long inputFirst, long inputLast, bool inputInclusive)
        {
            Range<long> input = new Range<long>(inputFirst, inputLast, inputInclusive);

            RangePartitioningResult<long> result = await Int64KeyRangePartitioner.Default.SplitAsync(input, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(input.First));
            Assert.That(result.Updated.Last, Is.EqualTo(input.Last));
            Assert.That(result.Updated.IsInclusive, Is.EqualTo(input.IsInclusive));

            Assert.That(result.Success, Is.False);
            Assert.That(result.Created, Is.Empty);
        }
    }
}
