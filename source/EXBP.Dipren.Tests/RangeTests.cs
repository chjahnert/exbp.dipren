
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
    }
}
