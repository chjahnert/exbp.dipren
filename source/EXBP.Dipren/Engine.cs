
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a processing engine that can iterate over a large set of ordered items in a distributed fashion.
    /// </summary>
    public class Engine
    {
        private readonly string _id;
        private readonly Configuration _configuration;


        /// <summary>
        ///   Gets the unique identifier of the current distributed processing engine instance.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that uniquely identifies the current engine instance.
        /// </value>
        public string Id => this._id;

        /// <summary>
        ///   Gets the configuration settings for the current distributed processing engine instance.
        /// </summary>
        public Configuration Configuration => this._configuration;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="configuration">
        ///   The configuration settings to use.
        /// </param>
        public Engine(Configuration configuration)
        {
            Assert.ArgumentIsNotNull(configuration, nameof(configuration));

            this._id = Engine.GenerateEngineIdentifier();
            this._configuration = configuration;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier to use for the engine instance.
        /// </param>
        /// <param name="configuration">
        ///   The configuration settings to use.
        /// </param>
        public Engine(string id, Configuration configuration)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));
            Assert.ArgumentIsNotNull(configuration, nameof(configuration));

            this._id = id;
            this._configuration = configuration;
        }

        /// <summary>
        ///   Starts a distributed processing job.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of the item key.
        /// </typeparam>
        /// <typeparam name="TItem">
        ///   The type of items to process.
        /// </typeparam>
        /// <param name="job">
        ///   The job to start.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation and can be used to access the result.
        /// </returns>
        public Task StartAsync<TKey, TItem>(Job<TKey, TItem> job, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///   Generates a unique identifier for an engine instance.
        /// </summary>
        /// <returns>
        ///   The unique identifier generated.
        /// </returns>
        private static string GenerateEngineIdentifier()
            => "machine-name/process-path/pid/instance";
    }
}
