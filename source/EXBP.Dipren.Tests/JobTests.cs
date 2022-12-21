
using EXBP.Dipren.Data;

using NSubstitute;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class JobTests
    {
        private readonly string _defaultId = "DPJ-0001";


        [Test]
        public void Ctor_ArgumentIdIsNull_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IRangePartitioner<int> partitioner = Substitute.For<IRangePartitioner<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(null, source, partitioner, serializer, processor));
        }

        [Test]
        public void Ctor_ArgumentSourceIsNull_ThrowsExcption()
        {
            IRangePartitioner<int> partitioner = Substitute.For<IRangePartitioner<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, null, partitioner, serializer, processor));
        }

        [Test]
        public void Ctor_ArgumentPartitionerIsNull_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, source, null, serializer, processor));
        }

        [Test]
        public void Ctor_ArgumentSerializerIsNull_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IRangePartitioner<int> partitioner = Substitute.For<IRangePartitioner<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, source, partitioner, null, processor));
        }

        [Test]
        public void Ctor_ArgumentProcessorIsNull_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IRangePartitioner<int> partitioner = Substitute.For<IRangePartitioner<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, source, partitioner, serializer, null));
        }
    }
}
