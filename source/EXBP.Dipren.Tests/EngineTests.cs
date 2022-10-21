
using EXBP.Dipren.Data.Memory;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class EngineTests
    {
        [Test]
        public void Ctor_ArgumentStoreIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new Engine(null));
        }

        [Test]
        public void RunAsync_ArgumentJobIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Engine engine = new Engine(store);

            Assert.ThrowsAsync<ArgumentNullException>(() => engine.RunAsync<int, int>(null, false, CancellationToken.None));
        }


        private class Int32SequenceDataSource : IDataSource<int, string>
        {
            private readonly int _minimum;
            private readonly int _maximum;


            public Int32SequenceDataSource(int minimum, int maximum)
            {
                this._minimum = minimum;
                this._maximum = maximum;
            }


            public Task<long> EstimateRangeSizeAsync(Range<int> range, CancellationToken canellation)
                => Task.FromResult<long>(Math.Abs(range.Last - range.First));

            public Task<Range<int>> GetEntireRangeAsync(CancellationToken cancellation)
                => Task.FromResult(new Range<int>(this._minimum, this._maximum, true));

            public Task<IEnumerable<KeyValuePair<int, string>>> GetNextBatchAsync(Range<int> range, int skip, int take, CancellationToken canellation)
            {
                List<KeyValuePair<int, string>> result = new List<KeyValuePair<int, string>>(take);

                if (range.IsAscending == true)
                {
                    int start = (range.First + skip);

                    for (int i = start; (i < (start + take)) && ((range.IsInclusive == true) && (i <= range.Last) || (range.IsInclusive == false) && (i < range.Last)); i++)
                    {
                        KeyValuePair<int, string> item = this.CreateItem(i);

                        result.Add(item);
                    }
                }
                else
                {
                    int start = (range.First - skip);

                    for (int i = start; (i > (start - take)) && ((range.IsInclusive == true) && (i >= range.Last) || (range.IsInclusive == false) && (i > range.Last)); i--)
                    {
                        KeyValuePair<int, string> item = this.CreateItem(i);

                        result.Add(item);
                    }
                }

                return Task.FromResult<IEnumerable<KeyValuePair<int, string>>>(result);
            }

            private KeyValuePair<int, string> CreateItem(int key)
            {
                string value = string.Format("{0:X8}", key);
                KeyValuePair<int, string> result = new KeyValuePair<int, string>(key, value);

                return result;
            }
        }
    }
}
