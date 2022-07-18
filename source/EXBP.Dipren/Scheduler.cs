
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements methods to schedule, cancel, and monitor distributed processing jobs.
    /// </summary>
    public class Scheduler
    {
        private readonly IEngineDataStore _store;
        private readonly Configuration _configuration;


        /// <summary>
        ///   Gets the configuration settings for the current distributed processing engine instance.
        /// </summary>
        public Configuration Configuration => this._configuration;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="configuration">
        ///   The configuration settings to use.
        /// </param>
        public Scheduler(IEngineDataStore store, Configuration configuration)
        {
            Assert.ArgumentIsNotNull(store, nameof(store));
            Assert.ArgumentIsNotNull(configuration, nameof(configuration));

            this._store = store;
            this._configuration = configuration;
        }


        /// <summary>
        ///   Schedules a distributed processing job.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TItem"></typeparam>
        /// <param name="job"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task ScheduleAsync<TKey, TItem>(Job<TKey, TItem> job, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            throw new NotImplementedException();
        }
    }
}
