
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Memory;

using NSubstitute;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class SchedulerTests
    {
        [Test]
        public void Ctor_ArgumentStoreIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new Scheduler(null));
        }

        [Test]
        public void Ctor_ArgumentClockIsNull_ThrowsException()
        {
            IEngineDataStore store = Substitute.For<IEngineDataStore>();

            Assert.Throws<ArgumentNullException>(() => new Scheduler(store, null));
        }

        [Test]
        public void ScheduleAsync_ArgumentJobIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Scheduler scheduler = new Scheduler(store);


            Assert.ThrowsAsync<ArgumentNullException>( () => scheduler.ScheduleAsync<int, int>(null, CancellationToken.None));
        }

        [Test]
        public async Task ScheduleAsync_ArgumentJobIsValid_SchedulesJob()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Scheduler scheduler = new Scheduler(store);

            DummyDataSource source = new DummyDataSource(1, 1024);
            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            Int32KeySerializer serializer = new Int32KeySerializer();
            IBatchProcessor<int> processor = Substitute.For<IBatchProcessor<int>>();
            TimeSpan timeout = TimeSpan.FromMinutes(3);

            Job<int, int> job = new Job<int, int>("DPJ-0001", source, arithmetics, serializer, processor, timeout, 16);

            await scheduler.ScheduleAsync(job, CancellationToken.None);

            Job sj = store.Jobs.First(j => j.Id == job.Id);

            Assert.That(sj.Exception, Is.Null);

            Partition sp = store.Partitions.First(p => p.JobId == job.Id);

            Assert.That(sp.Owner, Is.Null);
            Assert.That(sp.First, Is.EqualTo("1"));
            Assert.That(sp.Last, Is.EqualTo("1024"));
            Assert.That(sp.IsInclusive, Is.True);
            Assert.That(sp.Position, Is.EqualTo("0"));
            Assert.That(sp.Processed, Is.EqualTo(0L));
            Assert.That(sp.Remaining, Is.EqualTo(1024L));
            Assert.That(sp.IsSplitRequested, Is.False);
        }

        [Test]
        public void ScheduleAsync_RangeQueryFails_CreatesJobInFailedState()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Scheduler scheduler = new Scheduler(store);

            IDataSource<int, int> source = Substitute.For<IDataSource<int,int>>();

            source
                .When(x => x.GetEntireRangeAsync(Arg.Any<CancellationToken>()))
                .Do(x => { throw new ArgumentOutOfRangeException(); });

            source
                .When(x => x.EstimateRangeSizeAsync(Arg.Any<Range<int>>(), Arg.Any<CancellationToken>()))
                .Do(x => Task.FromResult(1000L));

            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            Int32KeySerializer serializer = new Int32KeySerializer();
            IBatchProcessor<int> processor = Substitute.For<IBatchProcessor<int>>();
            TimeSpan timeout = TimeSpan.FromMinutes(3);

            Job<int, int> job = new Job<int, int>("DPJ-0001", source, arithmetics, serializer, processor, timeout, 16);

            Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => scheduler.ScheduleAsync(job, CancellationToken.None));

            Job sj = store.Jobs.First(j => j.Id == job.Id);

            Assert.That(sj.Exception, Is.InstanceOf<ArgumentOutOfRangeException>());

            Assert.That(store.Partitions.Any(p => p.JobId == job.Id), Is.False);
        }

        [Test]
        public void ScheduleAsync_RangeSizeEstimationFails_CreatesJobInFailedState()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Scheduler scheduler = new Scheduler(store);

            IDataSource<int, int> source = Substitute.For<IDataSource<int, int>>();

            source
                .When(x => x.GetEntireRangeAsync(Arg.Any<CancellationToken>()))
                .Do(x => new Range<int>(1, 1000, true));

            source
                .When(x => x.EstimateRangeSizeAsync(Arg.Any<Range<int>>(), Arg.Any<CancellationToken>()))
                .Do(x => { throw new KeyNotFoundException(); });

            IKeyArithmetics<int> arithmetics = Substitute.For<IKeyArithmetics<int>>();
            Int32KeySerializer serializer = new Int32KeySerializer();
            IBatchProcessor<int> processor = Substitute.For<IBatchProcessor<int>>();
            TimeSpan timeout = TimeSpan.FromMinutes(3);

            Job<int, int> job = new Job<int, int>("DPJ-0001", source, arithmetics, serializer, processor, timeout, 16);

            Assert.ThrowsAsync<KeyNotFoundException>(() => scheduler.ScheduleAsync(job, CancellationToken.None));

            Job sj = store.Jobs.First(j => j.Id == job.Id);

            Assert.That(sj.Exception, Is.InstanceOf<KeyNotFoundException>());

            Assert.That(store.Partitions.Any(p => p.JobId == job.Id), Is.False);
        }

        private class DummyDataSource : IDataSource<int, int>
        {
            private readonly int _minimum;
            private readonly int _maximum;


            public DummyDataSource(int minimum = 0, int maximum = 1024)
            {
                this._minimum = Math.Min(minimum, maximum);
                this._maximum = Math.Max(minimum, maximum);
            }

            public Task<Range<int>> GetEntireRangeAsync(CancellationToken cancellation)
                => Task.FromResult(new Range<int>(this._minimum, this._maximum, true));

            public Task<long> EstimateRangeSizeAsync(Range<int> range, CancellationToken canellation)
            {
                if (range == null)
                {
                    throw new ArgumentNullException(nameof(range));
                }

                long result = Math.Abs(range.Last - range.First);

                if (range.IsInclusive)
                {
                    result += 1;
                }

                return Task.FromResult(result);
            }

            public Task<IEnumerable<KeyValuePair<int, int>>> GetNextBatchAsync(int last, int limit, CancellationToken canellation)
            {
                throw new NotImplementedException();
            }
        }
    }
}
