
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class StringKeyArithmeticsTests
    {
        [TestCase("0123456789", 3, "3", "7", true, "5")]
        [TestCase("1ab", 3, "a1", "aba", true, "aa1")]
        public void Split(string characterset, int length, string first, string last, bool inclusive, string expected)
        {
            Range<string> whole = new Range<string>(first, last, inclusive);

            StringKeyArithmetics arithmetics = new StringKeyArithmetics(characterset, length);

            Range<string> updated = arithmetics.Split(whole, out Range<string> created);

            Assert.That(updated, Is.Not.Null);
            Assert.That(updated.First, Is.EqualTo(first));
            Assert.That(updated.Last, Is.EqualTo(expected));
            Assert.That(updated.IsInclusive, Is.False);

            Assert.That(created, Is.Not.Null);
            Assert.That(created.First, Is.EqualTo(expected));
            Assert.That(created.Last, Is.EqualTo(last));
            Assert.That(created.IsInclusive, Is.EqualTo(inclusive));
        }
    }
}
