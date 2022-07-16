
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class RangeDefintionTests
    {
        [Test]
        public void Ctor_ArgumentFirstIsNull_ThrowsExcption()
        {
            Assert.Throws<ArgumentNullException>(() => new RangeDefinition<string>(null, "z"));
        }

        [Test]
        public void Ctor_ArgumentLastIsNull_ThrowsExcption()
        {
            Assert.Throws<ArgumentNullException>(() => new RangeDefinition<string>("a", null));
        }
    }
}
