
using EXBP.Dipren.Resilience;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Resilience
{
    [TestFixture]
    public class ExponentialBackoffDelyaProviderTests
    {
        [TestCase(1, 1, 1)]
        [TestCase(1, 2, 2)]
        [TestCase(1, 3, 4)]
        [TestCase(1, 4, 8)]
        [TestCase(1, 5, 16)]
        [TestCase(3, 1, 3)]
        [TestCase(3, 3, 12)]
        [TestCase(3, 5, 48)]
        public void GetDelay_ArgumentAttemptIsValid_ReturnsCorrectDelay(int initial, int attempt, int expected)
        {
            TimeSpan delay = TimeSpan.FromMilliseconds(initial);
            ExponentialBackoffDelayProvider provider = new ExponentialBackoffDelayProvider(delay);

            TimeSpan result = provider.GetDelay(attempt);
            TimeSpan correct = TimeSpan.FromMilliseconds(expected);

            Assert.That(result, Is.EqualTo(correct));
        }

        [Test]
        public void GetDelay_ArgumentAttemptIsNegative_ThrowsException()
        {
            TimeSpan delay = TimeSpan.FromMilliseconds(-1);

            Assert.Throws<ArgumentOutOfRangeException>(() => new ExponentialBackoffDelayProvider(delay));
        }
    }
}
