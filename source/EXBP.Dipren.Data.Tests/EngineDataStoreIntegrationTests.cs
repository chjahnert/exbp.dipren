
using System.Diagnostics;

using NUnit.Framework;

namespace EXBP.Dipren.Data.Tests
{
    public abstract class EngineDataStoreIntegrationTests
    {
        protected virtual string JobName { get; } = "cuboid-001";

        protected virtual TimeSpan BatchProcessingDuration { get; } = TimeSpan.Zero;

        protected virtual TimeSpan ProcessingTimeout { get; } = TimeSpan.FromMilliseconds(1000);


        protected abstract Task<IEngineDataStore> OnCreateEngineDataStoreAsync();

        private async Task<EngineDataStoreWrapper> CreateEngineDataStoreAsync()
        {
            IEngineDataStore store = await this.OnCreateEngineDataStoreAsync();
            EngineDataStoreWrapper result = new EngineDataStoreWrapper(store);

            return result;
        }

        protected virtual Job<int, Cuboid> OnCreateDistributeProcessingJob()
        {
            IDataSource<int, Cuboid> source = new CuboidDataSource(1, 100000);
            IBatchProcessor<Cuboid> processor = new CuboidBatchProcessor(this.BatchProcessingDuration);

            Job<int, Cuboid> result = new Job<int, Cuboid>(JobName, source, Int32KeyRangePartitioner.Default, Int32KeySerializer.Default, processor);

            return result;
        }

        protected virtual Settings OnCreateDistributeProcessingJobSettings()
            => new Settings(32, this.ProcessingTimeout, TimeSpan.Zero);


        private async Task ScheduleJobAsync()
        {
            IEngineDataStore store = await this.CreateEngineDataStoreAsync();

            Scheduler scheduler = new Scheduler(store);

            Job<int, Cuboid> job = this.OnCreateDistributeProcessingJob();
            Settings settings = this.OnCreateDistributeProcessingJobSettings();

            await scheduler.ScheduleAsync(job, settings);
        }

        [Test]
        [Explicit]
        public async Task RunJobAsync()
        {
            await this.ScheduleJobAsync();




        }


        [DebuggerDisplay("ID = {Id}, Dimensions = {Width} × {Height} × {Depth} ")]
        protected class Cuboid
        {
            public int Id { get; }

            public double Width { get; }

            public double Height { get; }

            public double Depth { get; }


            public Cuboid(int id, double width, double height, double depth)
            {
                this.Id = id;
                this.Width = width;
                this.Height = height;
                this.Depth = depth;
            }
        }


        private class CuboidDataSource : IDataSource<int, Cuboid>
        {
            private readonly int _minimum;
            private readonly int _maximum;


            public CuboidDataSource(int minimum, int maximum)
            {
                this._minimum = minimum;
                this._maximum = maximum;
            }


            public Task<long> EstimateRangeSizeAsync(Range<int> range, CancellationToken canellation)
                => Task.FromResult<long>(Math.Abs(range.Last - range.First) + ((range.IsInclusive == true) ? 1 : 0));

            public Task<Range<int>> GetEntireRangeAsync(CancellationToken cancellation)
                => Task.FromResult(new Range<int>(this._minimum, this._maximum, true));

            public Task<IEnumerable<KeyValuePair<int, Cuboid>>> GetNextBatchAsync(Range<int> range, int skip, int take, CancellationToken cancellation)
            {
                List<KeyValuePair<int, Cuboid>> result = new List<KeyValuePair<int, Cuboid>>(take);

                bool ascending = (range.First < range.Last);

                if (ascending == true)
                {
                    int start = (range.First + skip);

                    for (int i = start; (i < (start + take)) && ((range.IsInclusive == true) && (i <= range.Last) || (range.IsInclusive == false) && (i < range.Last)); i++)
                    {
                        KeyValuePair<int, Cuboid> item = this.CreateBatchItem(i);

                        result.Add(item);
                    }
                }
                else
                {
                    int start = (range.First - skip);

                    for (int i = start; (i > (start - take)) && ((range.IsInclusive == true) && (i >= range.Last) || (range.IsInclusive == false) && (i > range.Last)); i--)
                    {
                        KeyValuePair<int, Cuboid> item = this.CreateBatchItem(i);

                        result.Add(item);
                    }
                }

                return Task.FromResult<IEnumerable<KeyValuePair<int, Cuboid>>>(result);
            }

            private KeyValuePair<int, Cuboid> CreateBatchItem(int key)
            {
                double width = Random.Shared.NextDouble();
                double height = Random.Shared.NextDouble();
                double depth = Random.Shared.NextDouble();

                Cuboid value = new Cuboid(key, width, height, depth);

                KeyValuePair<int, Cuboid> result = new KeyValuePair<int, Cuboid>(key, value);

                return result;
            }
        }


        private class CuboidBatchProcessor : IBatchProcessor<Cuboid>
        {
            private readonly TimeSpan _duration;


            public CuboidBatchProcessor(TimeSpan duration)
            {
                this._duration = duration;
            }


            public async Task ProcessAsync(IEnumerable<Cuboid> items, CancellationToken cancellation)
            {
                if (this._duration != TimeSpan.Zero)
                {
                    await Task.Delay(this._duration);
                }
            }
        }
    }
}
