
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class RangeTests
    {
        [Test]
        public void Ctor_ArgumentFirstIsNull_ThrowsExcption()
        {
            Assert.Throws<ArgumentNullException>(() => new Range<string>(null, "z"));
        }

        [Test]
        public void Ctor_ArgumentLastIsNull_ThrowsExcption()
        {
            Assert.Throws<ArgumentNullException>(() => new Range<string>("a", null));
        }

        [Test]
        public void IsAscendingProperty_RangeIsAscending_ReturnsTrue()
        {
            Range<int> range = new Range<int>(32, 64);

            Assert.That(range.IsAscending, Is.True);
        }

        [Test]
        public void IsAscendingProperty_RangeIsDescending_ReturnsFalse()
        {
            Range<int> range = new Range<int>(64, 32);

            Assert.That(range.IsAscending, Is.False);
        }
    }
}
