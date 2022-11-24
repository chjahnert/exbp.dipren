
using EXBP.Dipren.Diagnostics;
using EXBP.Dipren.Resilience;


namespace EXBP.Dipren.Data.Postgres
{
    /// <summary>
    ///   Implements an <see cref="IEngineDataStore"/> that uses Postgres SQL as its storage engine and uses a backoff
    ///   retry policy for resilience.
    /// </summary>
    /// <remarks>
    ///   The implementation assumes that the required schema and table structure is already deployed.
    /// </remarks>
    public class PostgresEngineDataStore : ResilientEngineDataStore, IDisposable, IAsyncDisposable
    {
        private const int DEFAULT_RETRY_LIMIT = 16;
        private const int DEFAULT_RETRY_DELAY = 20;


        private static IErrorConditionClassifier DefaultErrorClassifier = new DbErrorConditionClassifier(false);

        private readonly PostgresEngineDataStoreImplementation _store;
        private readonly IAsyncRetryStrategy _strategy;


        /// <summary>
        ///   Gets the engine data store instance being wrapped.
        /// </summary>
        /// <value>
        ///   The <see cref="IEngineDataStore"/> instance being wrapped.
        /// </value>
        protected override IEngineDataStore Store => this._store;

        /// <summary>
        ///   Gets the <see cref="IAsyncRetryStrategy"/> object that implements the retry strategy to use.
        /// </summary>
        /// <value>
        ///   The <see cref="IAsyncRetryStrategy"/> instance that implements the retry strategy to use.
        /// </value>
        protected override IAsyncRetryStrategy Strategy => this._strategy;


        /// <summary>
        ///   Initializes a new instance of the <see cref="PostgresEngineDataStore"/> class.
        /// </summary>
        /// <param name="connectionString">
        ///   The connection string to use when connecting to the database.
        /// </param>
        /// <param name="retryLimit">
        ///   The number of retry attempts to perform in case a transient error occurs.
        /// </param>
        public PostgresEngineDataStore(string connectionString, int retryLimit = DEFAULT_RETRY_LIMIT) : this(connectionString, retryLimit, TimeSpan.FromMilliseconds(DEFAULT_RETRY_DELAY))
        {
            TimeSpan retryDelay = TimeSpan.FromMilliseconds(DEFAULT_RETRY_DELAY);
            IBackoffDelayProvider backoffDelayProvider = new ConstantBackoffDelayProvider(retryDelay);

            this._store = new PostgresEngineDataStoreImplementation(connectionString);
            this._strategy = new BackoffRetryStrategy(retryLimit, backoffDelayProvider, DefaultErrorClassifier);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="PostgresEngineDataStore"/> class.
        /// </summary>
        /// <param name="connectionString">
        ///   The connection string to use when connecting to the database.
        /// </param>
        /// <param name="retryLimit">
        ///   The number of retry attempts to perform in case a transient error occurs.
        /// </param>
        /// <param name="retryDelay">
        ///   The duration to wait before the first retry attempt. The value is doubled for each subsequent retry
        ///   attempt.
        /// </param>
        public PostgresEngineDataStore(string connectionString, int retryLimit, TimeSpan retryDelay)
        {
            IBackoffDelayProvider backoffDelayProvider = new ConstantBackoffDelayProvider(retryDelay);

            this._store = new PostgresEngineDataStoreImplementation(connectionString);
            this._strategy = new BackoffRetryStrategy(retryLimit, backoffDelayProvider, DefaultErrorClassifier);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="PostgresEngineDataStore"/> class.
        /// </summary>
        /// <param name="connectionString">
        ///   The connection string to use when connecting to the database.
        /// </param>
        /// <param name="retryStrategy">
        ///   The <see cref="IAsyncRetryStrategy"/> instance that implements the retry strategy to use.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="retryStrategy"/> is a <see langword="null"/> reference.
        /// </exception>
        public PostgresEngineDataStore(string connectionString, IAsyncRetryStrategy retryStrategy)
        {
            Assert.ArgumentIsNotNull(retryStrategy, nameof(retryStrategy));

            this._store = new PostgresEngineDataStoreImplementation(connectionString);
            this._strategy = retryStrategy;
        }


        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this._store.Dispose();

            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources asynchronously.
        /// </summary>
        /// <returns>
        ///   A <see cref="ValueTask"/> value that represents the asynchronous dispose operation.
        /// </returns>
        public async ValueTask DisposeAsync()
        {
            await this._store.DisposeAsync();

            GC.SuppressFinalize(this);
        }
    }
}
