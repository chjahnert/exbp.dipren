
using EXBP.Dipren.Resilience;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Resilience
{
    [TestFixture]
    public class PresetBackoffDelayProviderTests
    {
        [Test]
        public void Ctor_ArgumentDelaysIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new PresetBackoffDelayProvider(null));
        }

        [Test]
        public void Ctor_ArgumentDelaysIsEmpty_ThrowsException()
        {
            Assert.Throws<ArgumentException>(() => new PresetBackoffDelayProvider(new TimeSpan[0]));
        }

        [TestCase(-1, 1)]
        [TestCase(0, 1)]
        [TestCase(1, 1)]
        [TestCase(2, 2)]
        [TestCase(3, 3)]
        [TestCase(4, 3)]
        public void GetDelay_ArgumentAttemptIsValid_ReturnsCorrectDelay(int attempt, int expected)
        {
            TimeSpan[] preset = new TimeSpan[]
            {
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(3)
            };

            PresetBackoffDelayProvider provider = new PresetBackoffDelayProvider(preset);

            TimeSpan retruned = provider.GetDelay(attempt);

            Assert.That(retruned, Is.EqualTo(TimeSpan.FromSeconds(expected)));
        }
    }
}
