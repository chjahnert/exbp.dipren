
using NSubstitute;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class JobTests
    {
        private readonly string _defaultId = "DPJ-0001";
        private readonly TimeSpan _defaultTimeout = TimeSpan.FromSeconds(30.0);
        private readonly int _defaultBatchSize = 16;


        [Test]
        public void Ctor_ArgumentIdIsNull_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(null, source, arithmetics, serializer, processor, this._defaultTimeout, this._defaultBatchSize));
        }

        [Test]
        public void Ctor_ArgumentSourceIsNull_ThrowsExcption()
        {
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, null, arithmetics, serializer, processor, this._defaultTimeout, this._defaultBatchSize));
        }

        [Test]
        public void Ctor_ArgumentArithmeticsIsNull_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, source, null, serializer, processor, this._defaultTimeout, this._defaultBatchSize));
        }

        [Test]
        public void Ctor_ArgumentSerializerIsNull_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, source, arithmetics, null, processor, this._defaultTimeout, this._defaultBatchSize));
        }

        [Test]
        public void Ctor_ArgumentProcessorIsNull_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, source, arithmetics, serializer, null, this._defaultTimeout, this._defaultBatchSize));
        }

        [Test]
        public void Ctor_ArgumentTimeoutIsZero_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentOutOfRangeException>(() => new Job<int, string>(this._defaultId, source, arithmetics, serializer, processor, TimeSpan.Zero, this._defaultBatchSize));
        }

        [Test]
        public void Ctor_ArgumentTimeoutIsNegative_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            TimeSpan timeout = TimeSpan.FromSeconds(-1.0);

            Assert.Throws<ArgumentOutOfRangeException>(() => new Job<int, string>(this._defaultId, source, arithmetics, serializer, processor, timeout, this._defaultBatchSize));
        }

        [Test]
        public void Ctor_ArgumentBatchSizeIsZero_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentOutOfRangeException>(() => new Job<int, string>(this._defaultId, source, arithmetics, serializer, processor, this._defaultTimeout, 0));
        }

        [Test]
        public void Ctor_ArgumentBatchSizeIsNegative_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentOutOfRangeException>(() => new Job<int, string>(this._defaultId, source, arithmetics, serializer, processor, this._defaultTimeout, -1));
        }
    }
}
