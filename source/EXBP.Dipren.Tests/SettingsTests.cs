
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    internal class SettingsTests
    {

        [Test]
        public void Ctor_ArgumentTimeoutIsZero_ThrowsExcption()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new Settings(64, TimeSpan.Zero));
        }

        [Test]
        public void Ctor_ArgumentTimeoutIsNegative_ThrowsExcption()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(-1.0);

            Assert.Throws<ArgumentOutOfRangeException>(() => new Settings(64, timeout));
        }

        [Test]
        public void Ctor_ArgumentBatchSizeIsZero_ThrowsExcption()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new Settings(0, 250));
        }

        [Test]
        public void Ctor_ArgumentBatchSizeIsNegative_ThrowsExcption()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new Settings(-1, 250));
        }
    }
}
