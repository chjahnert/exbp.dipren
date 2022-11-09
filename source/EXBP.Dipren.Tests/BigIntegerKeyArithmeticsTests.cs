
using System.Collections;
using System.Numerics;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class BigIntegerKeyArithmeticsTests
    {
        [TestCaseSource(nameof(Split_ArgumentRangeIsSplittable_ParameterSource))]
        public void Split_ArgumentRangeIsSplittable_SplitsRangeCorrectly(BigInteger inputFirst, BigInteger inputLast, bool inputInclusive, BigInteger returnedFirst, BigInteger returnedLast, BigInteger createdFirst, BigInteger createdLast)
        {
            Range<BigInteger> input = new Range<BigInteger>(inputFirst, inputLast, inputInclusive);

            Range<BigInteger> returned = BigIntegerKeyArithmetics.Default.Split(input, out Range<BigInteger> created);

            Assert.That(returned.First, Is.EqualTo(returnedFirst));
            Assert.That(returned.Last, Is.EqualTo(returnedLast));
            Assert.That(returned.IsInclusive, Is.False);

            Assert.That(created.First, Is.EqualTo(createdFirst));
            Assert.That(created.Last, Is.EqualTo(createdLast));
            Assert.That(created.IsInclusive, Is.EqualTo(inputInclusive));
        }

        public static IEnumerable Split_ArgumentRangeIsSplittable_ParameterSource()
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

        [TestCaseSource(nameof(Split_ArgumentRangeIsNotSplittable_ParameterSource))]
        public void Split_ArgumentRangeIsNotSplittable_ReturnUnchangedRange(BigInteger inputFirst, BigInteger inputLast, bool inputInclusive)
        {
            Range<BigInteger> input = new Range<BigInteger>(inputFirst, inputLast, inputInclusive);

            Range<BigInteger> returned = BigIntegerKeyArithmetics.Default.Split(input, out Range<BigInteger> created);

            Assert.That(returned.First, Is.EqualTo(input.First));
            Assert.That(returned.Last, Is.EqualTo(input.Last));
            Assert.That(returned.IsInclusive, Is.EqualTo(input.IsInclusive));

            Assert.That(created, Is.Null);
        }

        public static IEnumerable Split_ArgumentRangeIsNotSplittable_ParameterSource()
        {
            yield return new object[] { new BigInteger(1), new BigInteger(1), true };
            yield return new object[] { new BigInteger(1), new BigInteger(2), true };
            yield return new object[] { new BigInteger(1), new BigInteger(3), false };
        }
    }
}
