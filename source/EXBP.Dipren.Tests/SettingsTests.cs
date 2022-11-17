
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

        [Test]
        public void Ctor_ArgumentMaximumClockDriftNotSpecified_PropertiesAreInitializedCorrectly()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(3D);
            Settings settings = new Settings(1, timeout);

            Assert.That(settings.BatchSize, Is.EqualTo(1));
            Assert.That(settings.Timeout, Is.EqualTo(timeout));
            Assert.That(settings.ClockDrift, Is.EqualTo(Settings.DefaultClockDrift));
        }

        [Test]
        public void Ctor_ArgumentMaximumClockDriftIsSpecified_PropertiesAreInitializedCorrectly()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(3D);
            TimeSpan maximumClockDrift = TimeSpan.FromSeconds(7D);

            Settings settings = new Settings(1, timeout, maximumClockDrift);

            Assert.That(settings.BatchSize, Is.EqualTo(1));
            Assert.That(settings.Timeout, Is.EqualTo(timeout));
            Assert.That(settings.ClockDrift, Is.EqualTo(maximumClockDrift));
        }
    }
}
