
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

        [TestCaseSource(nameof(SplitAsync_ArgumentRangeIsSplittable_ParameterSource))]
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

        [TestCaseSource(nameof(SplitAsync_ArgumentRangeIsNotSplittable_ParameterSource))]
        public void Split_ArgumentRangeIsNotSplittable_ReturnUnchangedRange(byte[] layout, Guid first, Guid last, bool inclusive)
        {
            Range<Guid> input = new Range<Guid>(first, last, inclusive);

            GuidKeyArithmetics arithemtics = new GuidKeyArithmetics(layout);

            Range<Guid> returned = arithemtics.Split(input, out Range<Guid> created);

            Assert.That(returned.First, Is.EqualTo(input.First));
            Assert.That(returned.Last, Is.EqualTo(input.Last));
            Assert.That(returned.IsInclusive, Is.EqualTo(input.IsInclusive));

            Assert.That(created, Is.Null);
        }

        [Test]
        public void SplitAsync_ArgumentRangeIsNull_ThrowsException()
        {
            GuidKeyArithmetics instance = new GuidKeyArithmetics(GuidLayout.MicrosoftSqlServer);

            Assert.ThrowsAsync<ArgumentNullException>(() => instance.SplitAsync(null, CancellationToken.None));
        }

        [TestCaseSource(nameof(SplitAsync_ArgumentRangeIsSplittable_ParameterSource))]
        public async Task SplitAsync_ArgumentRangeIsSplittable_SplitsRangeCorrectly(byte[] layout, Guid first, Guid last, bool inclusive, Guid mid)
        {
            Range<Guid> input = new Range<Guid>(first, last, inclusive);

            GuidKeyArithmetics arithemtics = new GuidKeyArithmetics(layout);

            RangePartitioningResult<Guid> result = await arithemtics.SplitAsync(input, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(first));
            Assert.That(result.Updated.Last, Is.EqualTo(mid));
            Assert.That(result.Updated.IsInclusive, Is.False);

            Assert.That(result.Success, Is.True);
            Assert.That(result.Created.Count, Is.EqualTo(1));

            Range<Guid> created = result.Created.First();

            Assert.That(created, Is.Not.Null);
            Assert.That(created.First, Is.EqualTo(mid));
            Assert.That(created.Last, Is.EqualTo(last));
            Assert.That(created.IsInclusive, Is.EqualTo(inclusive));
        }

        [TestCaseSource(nameof(SplitAsync_ArgumentRangeIsNotSplittable_ParameterSource))]
        public async Task SplitAsync_ArgumentRangeIsNotSplittable_ReturnUnchangedRange(byte[] layout, Guid first, Guid last, bool inclusive)
        {
            Range<Guid> input = new Range<Guid>(first, last, inclusive);

            GuidKeyArithmetics arithemtics = new GuidKeyArithmetics(layout);

            RangePartitioningResult<Guid> result = await arithemtics.SplitAsync(input, CancellationToken.None);

            Assert.That(result, Is.Not.Null);

            Assert.That(result.Updated.First, Is.EqualTo(input.First));
            Assert.That(result.Updated.Last, Is.EqualTo(input.Last));
            Assert.That(result.Updated.IsInclusive, Is.EqualTo(input.IsInclusive));

            Assert.That(result.Success, Is.False);
            Assert.That(result.Created, Is.Empty);
        }


        public static IEnumerable SplitAsync_ArgumentRangeIsSplittable_ParameterSource()
        {
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("00000000-0000-0000-0000-000000004000"), new Guid("00000000-0000-0000-0000-000000008000"), true, new Guid("00000000-0000-0000-0000-000000006000") };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("A0000000-0000-0000-0000-000000000000"), new Guid("E0000000-0000-0000-0000-000000000000"), false, new Guid("C0000000-0000-0000-0000-000000000000") };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("00000000-0000-0000-0000-000000008000"), new Guid("00000000-0000-0000-0000-000000004000"), true, new Guid("00000000-0000-0000-0000-000000006000") };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("E0000000-0000-0000-0000-000000000000"), new Guid("A0000000-0000-0000-0000-000000000000"), false, new Guid("C0000000-0000-0000-0000-000000000000") };

            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("00000000-0000-0000-0000-000000004000"), new Guid("00000000-0000-0000-0000-000000008000"), true, new Guid("00000000-0000-0000-0000-000000006000") };
            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("A0000000-0000-0000-0000-000000000000"), new Guid("E0000000-0000-0000-0000-000000000000"), false, new Guid("C0000000-0000-0000-0000-000000000000") };
            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("00000000-0000-0000-0000-000000008000"), new Guid("00000000-0000-0000-0000-000000004000"), true, new Guid("00000000-0000-0000-0000-000000006000") };
            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("E0000000-0000-0000-0000-000000000000"), new Guid("A0000000-0000-0000-0000-000000000000"), false, new Guid("C0000000-0000-0000-0000-000000000000") };

            yield return new object[] { GuidLayout.LexicographicalMemberwise, new Guid("00000000-0000-0000-0000-000000004000"), new Guid("00000000-0000-0000-0000-000000008000"), true, new Guid("00000000-0000-0000-0000-000000006000") };
            yield return new object[] { GuidLayout.LexicographicalMemberwise, new Guid("A0000000-0000-0000-0000-000000000000"), new Guid("E0000000-0000-0000-0000-000000000000"), false, new Guid("C0000000-0000-0000-0000-000000000000") };
            yield return new object[] { GuidLayout.LexicographicalMemberwise, new Guid("00000000-0000-0000-0000-000000008000"), new Guid("00000000-0000-0000-0000-000000004000"), true, new Guid("00000000-0000-0000-0000-000000006000") };
            yield return new object[] { GuidLayout.LexicographicalMemberwise, new Guid("E0000000-0000-0000-0000-000000000000"), new Guid("A0000000-0000-0000-0000-000000000000"), false, new Guid("C0000000-0000-0000-0000-000000000000") };
        }

        public static IEnumerable SplitAsync_ArgumentRangeIsNotSplittable_ParameterSource()
        {
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("04000000-0000-0000-0000-000000000000"), new Guid("04000000-0000-0000-0000-000000000000"), true };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("04000000-0000-0000-0000-000000000000"), new Guid("05000000-0000-0000-0000-000000000000"), true };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("04000000-0000-0000-0000-000000000000"), new Guid("06000000-0000-0000-0000-000000000000"), false };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("04000000-0000-0000-0000-000000000000"), new Guid("04000000-0000-0000-0000-000000000000"), true };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("05000000-0000-0000-0000-000000000000"), new Guid("04000000-0000-0000-0000-000000000000"), true };
            yield return new object[] { GuidLayout.MicrosoftSqlServer, new Guid("06000000-0000-0000-0000-000000000000"), new Guid("04000000-0000-0000-0000-000000000000"), false };

            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("00000000-0000-0000-0000-000000000004"), new Guid("00000000-0000-0000-0000-000000000004"), true };
            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("00000000-0000-0000-0000-000000000004"), new Guid("00000000-0000-0000-0000-000000000005"), true };
            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("00000000-0000-0000-0000-000000000004"), new Guid("00000000-0000-0000-0000-000000000006"), false };
            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("00000000-0000-0000-0000-000000000004"), new Guid("00000000-0000-0000-0000-000000000004"), true };
            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("00000000-0000-0000-0000-000000000005"), new Guid("00000000-0000-0000-0000-000000000004"), true };
            yield return new object[] { GuidLayout.LexicographicalBytewise, new Guid("00000000-0000-0000-0000-000000000006"), new Guid("00000000-0000-0000-0000-000000000004"), false };

            yield return new object[] { GuidLayout.LexicographicalMemberwise, new Guid("00000000-0000-0000-0000-000000000001"), new Guid("00000000-0000-0000-0000-000000000001"), true };
            yield return new object[] { GuidLayout.LexicographicalMemberwise, new Guid("00000000-0000-0000-0000-000000000001"), new Guid("00000000-0000-0000-0000-000000000002"), true };
            yield return new object[] { GuidLayout.LexicographicalMemberwise, new Guid("00000000-0000-0000-0000-000000000001"), new Guid("00000000-0000-0000-0000-000000000003"), false };
            yield return new object[] { GuidLayout.LexicographicalMemberwise, new Guid("00000000-0000-0000-0000-000000000003"), new Guid("00000000-0000-0000-0000-000000000002"), true };
            yield return new object[] { GuidLayout.LexicographicalMemberwise, new Guid("00000000-0000-0000-0000-000000000003"), new Guid("00000000-0000-0000-0000-000000000001"), false };
        }
    }
}
