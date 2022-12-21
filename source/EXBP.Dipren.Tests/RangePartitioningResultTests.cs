
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class RangePartitioningResultTests
    {
        [Test]
        public void Ctor_ArgumentUpdatedIsNull_ThrowsException()
        {
            IEnumerable<Range<int>> created = new List<Range<int>>
            {
                new Range<int>(1, 7, true)
            };

            Assert.Throws<ArgumentNullException>(() => new RangePartitioningResult<int>(null, created));
        }

        [Test]
        public void Ctor_ArgumentCreatedIsNull_ThrowsException()
        {
            Range<int> updated = new Range<int>(1, 7, true);

            Assert.Throws<ArgumentNullException>(() => new RangePartitioningResult<int>(updated, null));
        }

        [Test]
        public void SuccessProperty_PartitionsCreated_ReturnsTrue()
        {
            Range<int> updated = new Range<int>(1, 7, true);
            IEnumerable<Range<int>> created = new List<Range<int>>
            {
                new Range<int>(1, 7, true)
            };

            RangePartitioningResult<int> result = new RangePartitioningResult<int>(updated, created);

            Assert.That(result.Success, Is.True);
        }

        [Test]
        public void SuccessProperty_PartitionsNotCreated_ReturnsFalse()
        {
            Range<int> updated = new Range<int>(1, 7, true);
            IEnumerable<Range<int>> created = new List<Range<int>>();

            RangePartitioningResult<int> result = new RangePartitioningResult<int>(updated, created);

            Assert.That(result.Success, Is.False);
        }
    }
}
