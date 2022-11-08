
using System.Collections;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class GuidKeyArithmeticsTests
    {
        [Test]
        public void Ctor_ArgumentLayoutIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new GuidKeyArithmetics(null));
        }

        [TestCase(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 })]
        [TestCase(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 14 })]
        [TestCase(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 33 })]
        public void Ctor_ArgumentLayoutIsNotValid_ThrowsException(byte[] layout)
        {
            Assert.Throws<ArgumentException>(() => new GuidKeyArithmetics(layout));
        }

        [TestCaseSource(nameof(Split_ArgumentRangeIsSplittable_ParameterSource))]
        public void Split_ArgumentRangeIsSplittable_SplitsRangeCorrectly(byte[] layout, Guid first, Guid last, bool inclusive, Guid mid)
        {
            Range<Guid> range = new Range<Guid>(first, last, inclusive);

            GuidKeyArithmetics arithemtics = new GuidKeyArithmetics(layout);

            Range<Guid> updated = arithemtics.Split(range, out Range<Guid> created);

            Assert.That(updated, Is.Not.Null);
            Assert.That(updated.First, Is.EqualTo(first));
            Assert.That(updated.Last, Is.EqualTo(mid));
            Assert.That(updated.IsInclusive, Is.False);

            Assert.That(created, Is.Not.Null);
            Assert.That(created.First, Is.EqualTo(mid));
            Assert.That(created.Last, Is.EqualTo(last));
            Assert.That(created.IsInclusive, Is.EqualTo(inclusive));
        }

        public static IEnumerable Split_ArgumentRangeIsSplittable_ParameterSource()
        {
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("00000000-0000-0000-0000-000000004000"), new Guid("00000000-0000-0000-0000-000000008000"), true, new Guid("00000000-0000-0000-0000-000000006000") };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("A0000000-0000-0000-0000-000000000000"), new Guid("E0000000-0000-0000-0000-000000000000"), false, new Guid("C0000000-0000-0000-0000-000000000000") };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("00000000-0000-0000-0000-000000008000"), new Guid("00000000-0000-0000-0000-000000004000"), true, new Guid("00000000-0000-0000-0000-000000006000") };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("E0000000-0000-0000-0000-000000000000"), new Guid("A0000000-0000-0000-0000-000000000000"), false, new Guid("C0000000-0000-0000-0000-000000000000") };
        }
    }
}
