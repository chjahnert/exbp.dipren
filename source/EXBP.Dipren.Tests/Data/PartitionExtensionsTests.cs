
using System.Globalization;

using EXBP.Dipren.Data;

using NSubstitute;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data
{
    [TestFixture]
    public class PartitionExtensionsTests
    {
        private readonly IKeySerializer<int> _serializer;


        public PartitionExtensionsTests()
        {
            this._serializer = Substitute.For<IKeySerializer<int>>();

            this._serializer.Serialize(Arg.Any<int>()).Returns(x => ((int) x[0]).ToString(CultureInfo.InvariantCulture));
            this._serializer.Deserialize(Arg.Any<string>()).Returns(x => int.Parse((string) x[0], CultureInfo.InvariantCulture));
        }


        [Test]
        public void ToPartition_FlattenedPartiton_RetrunsCorrectValue()
        {
            Guid partitionId = Guid.NewGuid();
            const string jobId = "DPJ-0001";
            DateTime created = new DateTime(2022, 8, 1, 11, 32, 17, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 8, 1, 11, 36, 43, DateTimeKind.Utc);

            Partition source = new Partition(partitionId, jobId, created, updated, "1", "1024", true, "621", 621, 403, "machine-name/251/1/1442", true);
            Partition<int> target = source.ToPartition(this._serializer);

            Assert.That(target.Id, Is.EqualTo(source.Id));
            Assert.That(target.JobId, Is.EqualTo(source.JobId));
            Assert.That(target.Owner, Is.EqualTo(source.Owner));
            Assert.That(target.Created, Is.EqualTo(source.Created));
            Assert.That(target.Updated, Is.EqualTo(source.Updated));
            Assert.That(target.Range.First, Is.EqualTo(this._serializer.Deserialize(source.First)));
            Assert.That(target.Range.Last, Is.EqualTo(this._serializer.Deserialize(source.Last)));
            Assert.That(target.Range.IsInclusive, Is.EqualTo(source.IsInclusive));
            Assert.That(target.Position, Is.EqualTo(this._serializer.Deserialize(source.Position)));
            Assert.That(target.Processed, Is.EqualTo(source.Processed));
            Assert.That(target.Remaining, Is.EqualTo(source.Remaining));
            Assert.That(target.IsSplitRequested, Is.EqualTo(source.IsSplitRequested));
        }
    }
}
