
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    internal class EngineIdentifierTests
    {
        [Test]
        public void Generate_NoArguments_ReturnsCorrectValue()
        {
            string actual = EngineIdentifier.Generate();

            string expected = FormattableString.Invariant($"{Environment.MachineName}{EngineIdentifier.DELIMITER}{Environment.ProcessId}{EngineIdentifier.DELIMITER}{AppDomain.CurrentDomain.Id}{EngineIdentifier.DELIMITER}");

            StringAssert.StartsWith(expected, actual);
        }

        [Test]
        public void Generate_CalledMultipleTimes_ReturnsUniqueValue()
        {
            string a = EngineIdentifier.Generate();
            string b = EngineIdentifier.Generate();

            Assert.That(a, Is.Not.EqualTo(b));
        }
    }
}
