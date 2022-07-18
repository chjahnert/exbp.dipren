
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a processing engine that can iterate over a large set of ordered items in a distributed fashion.
    /// </summary>
    public class Engine
    {
        private readonly Configuration _configuration;
        private readonly NodeInfo _node;


        /// <summary>
        ///   Gets the configuration settings for the current distributed processing engine instance.
        /// </summary>
        public Configuration Configuration => this._configuration;

        /// <summary>
        ///   Gets an object that holds information about the current processing node.
        /// </summary>
        /// <value>
        ///   A <see cref="NodeInfo"/> object that holds information about the current processing node.
        /// </value>
        public NodeInfo Node => this._node;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="configuration">
        ///   The configuration settings to use.
        /// </param>
        /// <param name="node">
        ///   A <see cref="NodeInfo"/> object that holds information about the current processing node; or
        ///   <see langword="null"/> to use the default values.
        /// </param>
        public Engine(Configuration configuration, NodeInfo node = null)
        {
            Assert.ArgumentIsNotNull(configuration, nameof(configuration));

            this._configuration = configuration;
            this._node = (node ?? NodeInfo.Current);
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
    }
}
