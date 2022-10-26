
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    internal class NodeIdentifierTests
    {
        [Test]
        public void Generate_ArgumentsTypeIsUndefined_ThrowsException()
        {
            NodeType type = (NodeType) 33;

            Assert.Throws<ArgumentException>(() => NodeIdentifier.Generate(type));
        }

        [Test]
        public void Generate_ArgumentsTypeIsValid_ReturnsCorrectValue()
        {
            string actual = NodeIdentifier.Generate(NodeType.Scheduler);

            string expected = FormattableString.Invariant($"X{NodeIdentifier.TYPE_DELIMITER}{Environment.MachineName}{NodeIdentifier.ITEM_DELIMITER}{Environment.ProcessId}{NodeIdentifier.ITEM_DELIMITER}{AppDomain.CurrentDomain.Id}{NodeIdentifier.ITEM_DELIMITER}");

            StringAssert.StartsWith(expected, actual);
        }

        [Test]
        public void Generate_CalledMultipleTimes_ReturnsUniqueValue()
        {
            string a = NodeIdentifier.Generate(NodeType.Scheduler);
            string b = NodeIdentifier.Generate(NodeType.Scheduler);

            Assert.That(a, Is.Not.EqualTo(b));
        }
    }
}
