﻿
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

            Job<int, int> job = new Job<int, int>("Dummy", source, arithmetics, serializer, processor, timeout, 16);

            await scheduler.ScheduleAsync(job, CancellationToken.None);

            Job sj = store.Jobs.First(j => j.Id == job.Id);

            Assert.That(sj.Name, Is.EqualTo(job.Name));
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
