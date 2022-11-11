
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Memory;
using EXBP.Dipren.Telemetry;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class EngineTests
    {
        private IEventHandler DefaultEventHandler { get; } = new CompositeEventHandler(DebugEventLogger.Debug);


        [Test]
        public void Ctor_ArgumentStoreIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new Engine(null));
        }

        [Test]
        public void RunAsync_ArgumentJobIsNull_ThrowsException()
        {
            MemoryEngineDataStore store = new MemoryEngineDataStore();
            Engine engine = new Engine(store);

            Assert.ThrowsAsync<ArgumentNullException>(() => engine.RunAsync<int, int>(null, false));
        }

        [Test]
        public async Task RunAsync_SingleProcessingNodeAndValidJob_JobIsMarkedCompleted()
        {
            const string jobId = "DPJ-001";

            Int32SequenceDataSource source = new Int32SequenceDataSource(1, 8);
            CollectingBatchProcessor processor = new CollectingBatchProcessor();
            TimeSpan timeout = TimeSpan.FromSeconds(2);
            Job<int, string> job = new Job<int, string>(jobId, source, Int32KeyArithmetics.Default, Int32KeySerializer.Default, processor, timeout, 4);

            MemoryEngineDataStore store = new MemoryEngineDataStore();
            Scheduler scheduler = new Scheduler(store, this.DefaultEventHandler);

            await scheduler.ScheduleAsync(job);

            Engine engine = new Engine(store, this.DefaultEventHandler);

            await engine.RunAsync(job, false);

            Job persisted = await store.RetrieveJobAsync(jobId, CancellationToken.None);

            Assert.That(persisted.State, Is.EqualTo(JobState.Completed));
            Assert.That(persisted.Error, Is.Null);
        }

        [Test]
        public async Task RunAsync_SingleProcessingNodeAndRangeInAscendingOrder_ProcessesAllItems()
        {
            Int32SequenceDataSource source = new Int32SequenceDataSource(1, 128);
            CollectingBatchProcessor processor = new CollectingBatchProcessor();
            TimeSpan timeout = TimeSpan.FromSeconds(2);
            Job<int, string> job = new Job<int, string>("DPJ-001", source, Int32KeyArithmetics.Default, Int32KeySerializer.Default, processor, timeout, 4);

            MemoryEngineDataStore store = new MemoryEngineDataStore();
            Scheduler scheduler = new Scheduler(store, this.DefaultEventHandler);

            await scheduler.ScheduleAsync(job);

            Engine engine = new Engine(store, this.DefaultEventHandler);

            await engine.RunAsync(job, false);

            Assert.That(processor.Items.Count, Is.EqualTo(128));
            CollectionAssert.IsOrdered(processor.Items);
        }

        [Test]
        public async Task RunAsync_SingleProcessingNodeAndRangeInDescendingOrder_ProcessesAllItems()
        {
            Int32SequenceDataSource source = new Int32SequenceDataSource(128, 1);
            CollectingBatchProcessor processor = new CollectingBatchProcessor();
            TimeSpan timeout = TimeSpan.FromSeconds(2);
            Job<int, string> job = new Job<int, string>("DPJ-001", source, Int32KeyArithmetics.Default, Int32KeySerializer.Default, processor, timeout, 4);

            MemoryEngineDataStore store = new MemoryEngineDataStore();
            Scheduler scheduler = new Scheduler(store, this.DefaultEventHandler);

            await scheduler.ScheduleAsync(job);

            Engine engine = new Engine(store, this.DefaultEventHandler);

            await engine.RunAsync(job, false);

            Assert.That(processor.Items.Count, Is.EqualTo(128));
            CollectionAssert.IsOrdered(processor.Items.Reverse());
        }


        private class Int32SequenceDataSource : IDataSource<int, string>
        {
            private readonly int _minimum;
            private readonly int _maximum;
            private readonly IComparer<int> _comparer;


            public Int32SequenceDataSource(int minimum, int maximum, IComparer<int> comparer = null)
            {
                this._minimum = minimum;
                this._maximum = maximum;
                this._comparer = (comparer ?? Comparer<int>.Default);
            }


            public Task<long> EstimateRangeSizeAsync(Range<int> range, CancellationToken canellation)
                => Task.FromResult<long>(Math.Abs(range.Last - range.First) + ((range.IsInclusive == true) ? 1 : 0));

            public Task<Range<int>> GetEntireRangeAsync(CancellationToken cancellation)
                => Task.FromResult(new Range<int>(this._minimum, this._maximum, true));

            public Task<IEnumerable<KeyValuePair<int, string>>> GetNextBatchAsync(Range<int> range, int skip, int take, CancellationToken cancellation)
            {
                List<KeyValuePair<int, string>> result = new List<KeyValuePair<int, string>>(take);

                bool ascending = range.IsAscending(this._comparer);

                if (ascending == true)
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

        private class CollectingBatchProcessor : IBatchProcessor<string>
        {
            private readonly List<string> _items = new List<string>();

            public IReadOnlyList<string> Items => this._items;

            public Task ProcessAsync(IEnumerable<string> items, CancellationToken cancellation)
            {
                this._items.AddRange(items);

                return Task.CompletedTask;
            }
        }
    }
}
