
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class ConfigurationTests
    {
        [TestCase(1.0, 1.0)]
        [TestCase(-1.0, 1.0)]
        public void MaximumClockDriftProperty_ValueIsSpecified_RetrunsAbsoluteValue(double value, double expected)
        {
            Configuration configuration = new Configuration
            {
                MaximumClockDrift = TimeSpan.FromSeconds(value)
            };

            Assert.That(configuration.MaximumClockDrift, Is.EqualTo(TimeSpan.FromSeconds(expected)));
        }
    }
}
