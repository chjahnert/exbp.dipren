﻿
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    internal class Int32KeyArithmeticsTests
    {
        [Test]
        public void Split_ArgumentRangeIsNull_ThrowsException()
        {
            Int32KeyArithmetics arithmetics = new Int32KeyArithmetics();

            Assert.Throws<ArgumentNullException>(() => arithmetics.Split(null, out Range<int> _));
        }

        [TestCase(1, 8, true, 1, 5, 5, 8)]
        [TestCase(8, 1, true, 8, 4, 4, 1)]
        [TestCase(-7, 5, true, -7, -1, -1, 5)]
        [TestCase(5, -7, true, 5, -1, -1, -7)]
        [TestCase(1, 8, false, 1, 5, 5, 8)]
        [TestCase(8, 1, false, 8, 4, 4, 1)]
        [TestCase(-7, 5, false, -7, -1, -1, 5)]
        [TestCase(5, -7, false, 5, -1, -1, -7)]
        public void Split_ValidArguments_SplitsRangeCorrectly(int inputFirst, int inputLast, bool inputInclusive, int returnedFirst, int returnedLast, int createdFirst, int createdLast)
        {
            Int32KeyArithmetics arithmetics = new Int32KeyArithmetics();

            Range<int> input = new Range<int>(inputFirst, inputLast, inputInclusive);

            Range<int> returned = arithmetics.Split(input, out Range<int> created);

            Assert.That(returned.First, Is.EqualTo(returnedFirst));
            Assert.That(returned.Last, Is.EqualTo(returnedLast));
            Assert.That(returned.IsInclusive, Is.False);

            Assert.That(created.First, Is.EqualTo(createdFirst));
            Assert.That(created.Last, Is.EqualTo(createdLast));
            Assert.That(created.IsInclusive, Is.EqualTo(inputInclusive));
        }
    }
}