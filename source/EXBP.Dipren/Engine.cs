﻿
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a processing engine that can iterate over a large set of ordered items in a distributed fashion.
    /// </summary>
    public class Engine
    {
        private readonly string _id;
        private readonly IEngineDataStore _store;
        private readonly Configuration _configuration;


        /// <summary>
        ///   Gets the configuration settings for the current distributed processing engine instance.
        /// </summary>
        public Configuration Configuration => this._configuration;

        /// <summary>
        ///   Gets the unique identifier of the current engine.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the unique identifier of the current engine.
        /// </value>
        public string Identity => this._id;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="configuration">
        ///   The configuration settings to use.
        /// </param>
        public Engine(IEngineDataStore store, Configuration configuration)
        {
            Assert.ArgumentIsNotNull(store, nameof(store));
            Assert.ArgumentIsNotNull(configuration, nameof(configuration));

            IEngineIdentityProvider identity = new DefaultEngineIdentityProvider();

            this._store = store;
            this._configuration = configuration;
            this._id = identity.GetEngineIdentifier();
        }

        /// <summary>
        ///   Executes a distributed processing job.
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
        public Task ExecuteAsync<TKey, TItem>(Job<TKey, TItem> job, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            throw new NotImplementedException();
        }
    }
}
