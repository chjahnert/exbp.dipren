
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class EngineTests
    {
        [Test]
        public void Ctor_ArgumentStoreIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new Engine(null));
        }
    }
}
