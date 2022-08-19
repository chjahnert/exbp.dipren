
using EXBP.Dipren.Data;

using NSubstitute;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class SchedulerTests
    {
        [Test]
        public void Ctor_ArgumentStoreIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new Scheduler(null));
        }

        [Test]
        public void Ctor_ArgumentClockIsNull_ThrowsException()
        {
            IEngineDataStore store = Substitute.For<IEngineDataStore>();

            Assert.Throws<ArgumentNullException>(() => new Scheduler(store, null));
        }
    }
}
