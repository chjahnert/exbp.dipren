
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class JobDefintionTests
    {
        private readonly RangeDefinition<int> _rangeInt32 = new RangeDefinition<int>(16, 64);


        [Test]
        public void Ctor_ArgumentNameIsNull_ThrowsExcption()
        {
            Assert.Throws<ArgumentNullException>(() => new JobDefinition<int>(null, null, this._rangeInt32));
        }

        [Test]
        public void Ctor_ArgumentNameIsWhitespaceOnly_ThrowsExcption()
        {
            Assert.Throws<ArgumentException>(() => new JobDefinition<int>(" ", null, this._rangeInt32));
        }

        [Test]
        public void Ctor_ArgumentRangeIsNull_ThrowsExcption()
        {
            Assert.Throws<ArgumentNullException>(() => new JobDefinition<int>("A", null, null));
        }
    }
}
