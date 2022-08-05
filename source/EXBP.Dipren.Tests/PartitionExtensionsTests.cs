
using System.Globalization;

using NSubstitute;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
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
        public void Flatten_HydratedPartiton_RetrunsCorrectValue()
        {
            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 8, 1, 11, 32, 17, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 8, 1, 11, 36, 43, DateTimeKind.Utc);
            Range<int> range = new Range<int>(1, 1024, true);

            Partition<int> hydrated = new Partition<int>(id, "machine-name/251/1/1442", created, updated, range, 621, 621, 403);
            Partition dehydrated = hydrated.Dehydrate(this._serializer);

            Assert.That(dehydrated.Id, Is.EqualTo(hydrated.Id));
            Assert.That(dehydrated.Owner, Is.EqualTo(hydrated.Owner));
            Assert.That(dehydrated.Created, Is.EqualTo(hydrated.Created));
            Assert.That(dehydrated.Updated, Is.EqualTo(hydrated.Updated));
            Assert.That(dehydrated.First, Is.EqualTo(this._serializer.Serialize(hydrated.Range.First)));
            Assert.That(dehydrated.Last, Is.EqualTo(this._serializer.Serialize(hydrated.Range.Last)));
            Assert.That(dehydrated.IsInclusive, Is.EqualTo(hydrated.Range.IsInclusive));
            Assert.That(dehydrated.Position, Is.EqualTo(this._serializer.Serialize(hydrated.Position)));
            Assert.That(dehydrated.Processed, Is.EqualTo(hydrated.Processed));
            Assert.That(dehydrated.Remaining, Is.EqualTo(hydrated.Remaining));
        }

        [Test]
        public void Hydrate_DehydratedPartiton_RetrunsCorrectValue()
        {
            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 8, 1, 11, 32, 17, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 8, 1, 11, 36, 43, DateTimeKind.Utc);
            Range<int> range = new Range<int>(1, 1024, true);

            Partition dehydrated = new Partition(id, "machine-name/251/1/1442", created, updated, "1", "1024", true, "621", 621, 403);
            Partition<int> hydrated = dehydrated.Hydrate(this._serializer);

            Assert.That(hydrated.Id, Is.EqualTo(dehydrated.Id));
            Assert.That(hydrated.Owner, Is.EqualTo(dehydrated.Owner));
            Assert.That(hydrated.Created, Is.EqualTo(dehydrated.Created));
            Assert.That(hydrated.Updated, Is.EqualTo(dehydrated.Updated));
            Assert.That(hydrated.Range.First, Is.EqualTo(this._serializer.Deserialize(dehydrated.First)));
            Assert.That(hydrated.Range.Last, Is.EqualTo(this._serializer.Deserialize(dehydrated.Last)));
            Assert.That(hydrated.Range.IsInclusive, Is.EqualTo(dehydrated.IsInclusive));
            Assert.That(hydrated.Position, Is.EqualTo(this._serializer.Deserialize(dehydrated.Position)));
            Assert.That(hydrated.Processed, Is.EqualTo(dehydrated.Processed));
            Assert.That(hydrated.Remaining, Is.EqualTo(dehydrated.Remaining));
        }
    }
}
