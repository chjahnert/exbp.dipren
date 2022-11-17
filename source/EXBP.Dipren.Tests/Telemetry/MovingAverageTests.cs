
using EXBP.Dipren.Telemetry;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Telemetry
{
    [TestFixture]
    public class MovingAverageTests
    {
        [Test]
        public void AverageProperty_Empty_ReturnsCorrectAverage()
        {
            MovingAverage ma = new MovingAverage(4);

            Assert.That(ma.Average, Is.Zero);
        }

        [Test]
        public void AverageProperty_Partial_ReturnsCorrectAverage()
        {
            MovingAverage ma = new MovingAverage(4);

            ma.Add(3D);
            ma.Add(5D);

            Assert.That(ma.Average, Is.EqualTo(4D));
        }

        [Test]
        public void AverageProperty_Saturated_ReturnsCorrectAverage()
        {
            MovingAverage ma = new MovingAverage(4);

            ma.Add(1D);

            ma.Add(2D);
            ma.Add(5D);
            ma.Add(4D);
            ma.Add(5D);

            Assert.That(ma.Average, Is.EqualTo(4D));
        }

        [Test]
        public void CountProperty_Empty_ReturnsCorrectCount()
        {
            MovingAverage ma = new MovingAverage(4);

            Assert.That(ma.Count, Is.Zero);
        }

        [Test]
        public void CountProperty_Partial_ReturnsCorrectCount()
        {
            MovingAverage ma = new MovingAverage(4);

            ma.Add(3D);
            ma.Add(5D);

            Assert.That(ma.Count, Is.EqualTo(2));
        }

        [Test]
        public void CountProperty_Saturated_ReturnsCorrectCount()
        {
            MovingAverage ma = new MovingAverage(4);

            ma.Add(1D);

            ma.Add(2D);
            ma.Add(5D);
            ma.Add(4D);
            ma.Add(5D);

            Assert.That(ma.Count, Is.EqualTo(4));
        }
    }
}
