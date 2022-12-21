
using System.Collections;
using System.Numerics;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class BigIntegerKeyRangePartitionerTests
    {
        [Test]
        public void SplitAsync_ArgumentRangeIsNull_ThrowsException()
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => BigIntegerKeyRangePartitioner.Default.SplitAsync(null, CancellationToken.None));
        }

        [TestCaseSource(nameof(SplitAsync_ArgumentRangeIsSplittable_ParameterSource))]
        public async Task SplitAsync_ArgumentRangeIsSplittable_SplitsRangeCorrectly(BigInteger inputFirst, BigInteger inputLast, bool inputInclusive, BigInteger returnedFirst, BigInteger returnedLast, BigInteger createdFirst, BigInteger createdLast)
        {
            Range<BigInteger> input = new Range<BigInteger>(inputFirst, inputLast, inputInclusive);

            var result = await BigIntegerKeyRangePartitioner.Default.SplitAsync(input, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(returnedFirst));
            Assert.That(result.Updated.Last, Is.EqualTo(returnedLast));
            Assert.That(result.Updated.IsInclusive, Is.False);

            Assert.That(result.Success, Is.True);
            Assert.That(result.Created.Count, Is.EqualTo(1));

            Range<BigInteger> created = result.Created.First();

            Assert.That(created.First, Is.EqualTo(createdFirst));
            Assert.That(created.Last, Is.EqualTo(createdLast));
            Assert.That(created.IsInclusive, Is.EqualTo(inputInclusive));
        }

        [TestCaseSource(nameof(SplitAsync_ArgumentRangeIsNotSplittable_ParameterSource))]
        public async Task SplitAsync_ArgumentRangeIsNotSplittable_ReturnUnchangedRange(BigInteger inputFirst, BigInteger inputLast, bool inputInclusive)
        {
            Range<BigInteger> input = new Range<BigInteger>(inputFirst, inputLast, inputInclusive);

            RangePartitioningResult<BigInteger> result = await BigIntegerKeyRangePartitioner.Default.SplitAsync(input, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(input.First));
            Assert.That(result.Updated.Last, Is.EqualTo(input.Last));
            Assert.That(result.Updated.IsInclusive, Is.EqualTo(input.IsInclusive));

            Assert.That(result.Success, Is.False);
            Assert.That(result.Created, Is.Empty);
        }


        public static IEnumerable SplitAsync_ArgumentRangeIsSplittable_ParameterSource()
        {
            yield return new object[] { new BigInteger(1), new BigInteger(8), true, new BigInteger(1), new BigInteger(4), new BigInteger(4), new BigInteger(8) };
            yield return new object[] { new BigInteger(8), new BigInteger(1), true, new BigInteger(8), new BigInteger(5), new BigInteger(5), new BigInteger(1) };
            yield return new object[] { new BigInteger(-7), new BigInteger(5), true, new BigInteger(-7), new BigInteger(-1), new BigInteger(-1), new BigInteger(5) };
            yield return new object[] { new BigInteger(5), new BigInteger(-7), true, new BigInteger(5), new BigInteger(-1), new BigInteger(-1), new BigInteger(-7) };
            yield return new object[] { new BigInteger(1), new BigInteger(8), false, new BigInteger(1), new BigInteger(4), new BigInteger(4), new BigInteger(8) };
            yield return new object[] { new BigInteger(8), new BigInteger(1), false, new BigInteger(8), new BigInteger(5), new BigInteger(5), new BigInteger(1) };
            yield return new object[] { new BigInteger(-7), new BigInteger(5), false, new BigInteger(-7), new BigInteger(-1), new BigInteger(-1), new BigInteger(5) };
            yield return new object[] { new BigInteger(5), new BigInteger(-7), false, new BigInteger(5), new BigInteger(-1), new BigInteger(-1), new BigInteger(-7) };
        }

        public static IEnumerable SplitAsync_ArgumentRangeIsNotSplittable_ParameterSource()
        {
            yield return new object[] { new BigInteger(1), new BigInteger(1), true };
            yield return new object[] { new BigInteger(1), new BigInteger(2), true };
            yield return new object[] { new BigInteger(1), new BigInteger(3), false };
        }
    }
}
