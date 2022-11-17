
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
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(null, source, arithmetics, serializer, processor));
        }

        [Test]
        public void Ctor_ArgumentSourceIsNull_ThrowsExcption()
        {
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, null, arithmetics, serializer, processor));
        }

        [Test]
        public void Ctor_ArgumentArithmeticsIsNull_ThrowsExcption()
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
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IBatchProcessor<string> processor = Substitute.For<IBatchProcessor<string>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, source, arithmetics, null, processor));
        }

        [Test]
        public void Ctor_ArgumentProcessorIsNull_ThrowsExcption()
        {
            IDataSource<int, string> source = Substitute.For<IDataSource<int, string>>();
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            IKeySerializer<int> serializer = Substitute.For<IKeySerializer<int>>();

            Assert.Throws<ArgumentNullException>(() => new Job<int, string>(this._defaultId, source, arithmetics, serializer, null));
        }
    }
}
