
using EXBP.Dipren.Data.Memory;

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

        [Test]
        public void RunAsync_ArgumentJobIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Engine engine = new Engine(store);

            Assert.ThrowsAsync<ArgumentNullException>(() => engine.RunAsync<int, int>(null, false, CancellationToken.None));
        }
    }
}
