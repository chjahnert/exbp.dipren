
using System.Data.Common;

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


        private readonly PostgresEngineDataStoreImplementation _store;
        private readonly IAsyncRetryStrategy _strategy;
        private readonly TimeSpan _delay;


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
        public PostgresEngineDataStore(string connectionString, int retryLimit = DEFAULT_RETRY_LIMIT)
        {
            this._delay = TimeSpan.FromMilliseconds(DEFAULT_RETRY_DELAY);
            this._store = new PostgresEngineDataStoreImplementation(connectionString);
            this._strategy = new BackoffRetryStrategy(retryLimit, this.GetRetryWaitDuration, this.IsTransientError);
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
        public PostgresEngineDataStore(string connectionString, int retryLimit, TimeSpan retryDelay) : this(connectionString, retryLimit)
        {
            Assert.ArgumentIsGreater(retryDelay, TimeSpan.Zero, nameof(retryDelay));

            this._delay = retryDelay;
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

            this._delay = TimeSpan.Zero;
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


        /// <summary>
        ///   Returns the duration to wait before the next retry attempt.
        /// </summary>
        /// <param name="attempt">
        ///   The number of the next attempt.
        /// </param>
        /// <returns>
        ///   A <see cref="TimeSpan"/> value containing the duration to wait before the next retry attempt.
        /// </returns>
        private TimeSpan GetRetryWaitDuration(int attempt)
            => (this._delay * Math.Pow(2, attempt));

        /// <summary>
        ///   Determines whether the specified exception is a transient error.
        /// </summary>
        /// <param name="exception">
        ///   A <see cref="Exception"/> object providing details about the error condition.
        /// </param>
        /// <returns>
        ///   <see langword="true"/> if <paramref name="exception"/> is caused by a transient error condition;
        ///   otherwise <see langword="false"/>.
        /// </returns>
        /// <remarks>
        ///   The default implementation returns <see langword="false"/> unless the exception is of type
        ///   <see cref="DbException"/> and the <see cref="DbException.IsTransient"/> property is
        ///   <see langword="true"/>.
        /// </remarks>
        private bool IsTransientError(Exception exception)
            => (exception as DbException)?.IsTransient ?? false;
    }
}
