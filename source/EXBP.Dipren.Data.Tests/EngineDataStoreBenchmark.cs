
using System.Diagnostics;

using EXBP.Dipren.Diagnostics;
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren.Data.Tests
{
    public class EngineDataStoreBenchmark
    {
        private readonly IEngineDataStoreFactory _factory;
        private readonly EngineDataStoreBenchmarkSettings _settings;


        public EngineDataStoreBenchmark(IEngineDataStoreFactory factory, EngineDataStoreBenchmarkSettings settings)
        {
            Assert.ArgumentIsNotNull(factory, nameof(factory));
            Assert.ArgumentIsNotNull(settings, nameof(settings));

            this._factory = factory;
            this._settings = settings;
        }


        public async Task<EngineDataStoreBenchmarkRecording> RunAsync(CancellationToken cancellation = default)
        {
            string name = FormattableString.Invariant($"benchmark {DateTime.Now:yyyy-MM-dd HH.mm.ss} - {this._settings.Name}");

            CuboidBatchProcessor processor = new CuboidBatchProcessor(this._settings.BatchProcessingDuration);
            CollectingEventLogger collector = new CollectingEventLogger(EventSeverity.Information);

            await this.ScheduleJobAsync(name, processor, collector, cancellation);

            Task<IEnumerable<StatusReport>> monitor = Task.Run(async () => await this.MonitorJobAsync(name, cancellation));

            CollectingEventLogger[] collectors = new CollectingEventLogger[this._settings.ProcessingNodes];
            Task[] tasks = new Task[this._settings.ProcessingNodes];

            Stopwatch stopwatch = Stopwatch.StartNew();

            for (int i = 0; i < tasks.Length; i++)
            {
                if (this._settings.ProcessingNodeStartupDelay >= TimeSpan.Zero)
                {
                    await Task.Delay(this._settings.ProcessingNodeStartupDelay);
                }

                CollectingEventLogger handler = new CollectingEventLogger(EventSeverity.Information);

                tasks[i] = Task.Run(async () => await this.RunJobAsync(name, processor, handler, cancellation));
                collectors[i] = handler;
            }

            await Task.WhenAll(tasks);

            EngineDataStoreBenchmarkRecording result = new EngineDataStoreBenchmarkRecording
            {
                Id = name,
                Settings = this._settings,
                Processed = processor.Count,
                Duration = stopwatch.Elapsed,
                Snapshots = await monitor,
                Events = Enumerable.Concat(collector.Events, collectors.SelectMany(c => c.Events).OrderBy(e => e.Timestamp))
            };

            return result;
        }

        private async Task ScheduleJobAsync(string name, IBatchProcessor<Cuboid> processor, IEventHandler handler, CancellationToken cancellation)
        {
            IEngineDataStore store = await this._factory.CreateAsync(cancellation);

            await using (EngineDataStoreWrapper wrapped = new EngineDataStoreWrapper(store))
            {
                Scheduler scheduler = new Scheduler(wrapped, handler);

                Job<int, Cuboid> job = this.CreateDistributeProcessingJob(name, processor);
                Settings settings = this.CreateDistributeProcessingJobSettings();

                await scheduler.ScheduleAsync(job, settings);
            }
        }

        private async Task RunJobAsync(string name, IBatchProcessor<Cuboid> processor, IEventHandler handler, CancellationToken cancellation)
        {
            IEngineDataStore store = await this._factory.CreateAsync(cancellation);

            await using (EngineDataStoreWrapper wrapped = new EngineDataStoreWrapper(store))
            {
                Configuration configuration = this.OnCreateEngineConfiguration();
                CompositeEventHandler composite = new CompositeEventHandler(DebugEventLogger.Information, handler);
                Engine engine = new Engine(wrapped, configuration, composite);

                Job<int, Cuboid> job = this.CreateDistributeProcessingJob(name, processor);

                await engine.RunAsync(job, false, cancellation);
            }
        }

        private async Task<IEnumerable<StatusReport>> MonitorJobAsync(string name, CancellationToken cancellation)
        {
           List<StatusReport> result = new List<StatusReport>();

           IEngineDataStore store = await this._factory.CreateAsync(cancellation);

            await using (EngineDataStoreWrapper wrapped = new EngineDataStoreWrapper(store))
            {
                Scheduler scheduler = new Scheduler(wrapped);

                StatusReport summary = null;

                do
                {
                    try
                    {
                        summary = await scheduler.GetStatusReportAsync(name, CancellationToken.None);

                        result.Add(summary);
                    }
                    catch (UnknownIdentifierException)
                    {
                    }

                    await Task.Delay(this._settings.ReportingInterval);
                }
                while ((summary?.State != JobState.Completed) && (summary?.State != JobState.Failed));
            }

            return result;
        }

        protected virtual Job<int, Cuboid> CreateDistributeProcessingJob(string name, IBatchProcessor<Cuboid> processor)
        {
            IDataSource<int, Cuboid> source = new CuboidDataSource(1, this._settings.DatasetSize);
            Job<int, Cuboid> result = new Job<int, Cuboid>(name, source, Int32KeyRangePartitioner.Default, Int32KeySerializer.Default, processor);

            return result;
        }

        protected virtual Settings CreateDistributeProcessingJobSettings()
            => new Settings(this._settings.BatchSize, this._settings.ProcessingTimeout, TimeSpan.Zero);

        protected virtual Configuration OnCreateEngineConfiguration()
            => new Configuration(this._settings.PollingInterval);



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
            private long _count = 0L;


            public long Count => Interlocked.Read(ref this._count);


            public CuboidBatchProcessor(TimeSpan duration)
            {
                this._duration = duration;
            }


            public async Task ProcessAsync(IEnumerable<Cuboid> items, CancellationToken cancellation)
            {
                if (this._duration >= TimeSpan.Zero)
                {
                    await Task.Delay(this._duration);
                }

                int count = items.Count();

                Interlocked.Add(ref this._count, count);
            }

            public void Reset()
            {
                Interlocked.Exchange(ref this._count, 0L);
            }
        }


        private class CollectingEventLogger : IEventHandler
        {
            private readonly EventSeverity _level;
            private readonly List<EventDescriptor> _events = new List<EventDescriptor>();


            public IEnumerable<EventDescriptor> Events => _events;


            public CollectingEventLogger(EventSeverity level = EventSeverity.Information)
            {
                Assert.ArgumentIsDefined(level, nameof(level));

                this._level = level;
            }


            public virtual Task HandleEventAsync(EventDescriptor descriptor, CancellationToken cancellation)
            {
                Assert.ArgumentIsNotNull(descriptor, nameof(descriptor));

                if (descriptor.Severity >= this._level)
                {
                    this._events.Add(descriptor);
                }

                return Task.CompletedTask;
            }
        }
    }
}
