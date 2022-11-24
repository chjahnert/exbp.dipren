
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

        private static readonly TimeSpan DefaultRetryDelay = TimeSpan.FromMilliseconds(DEFAULT_RETRY_DELAY);


        /// <summary>
        ///   Gets the <see cref="IEngineDataStore"/> object being wrapped.
        /// </summary>
        /// <value>
        ///   The <see cref="IEngineDataStore"/> object being wrapped.
        /// </value>
        protected override IEngineDataStore Store { get; }


        /// <summary>
        ///   Initializes a new instance of the <see cref="PostgresEngineDataStore"/> class.
        /// </summary>
        /// <param name="connectionString">
        ///   The connection string to use when connecting to the database.
        /// </param>
        /// <param name="retryLimit">
        ///   The number of retry attempts to perform in case a transient error occurs. By default, operations are
        ///   retried up to 16 times.
        /// </param>
        public PostgresEngineDataStore(string connectionString, int retryLimit = DEFAULT_RETRY_LIMIT) : this(connectionString, retryLimit, DefaultRetryDelay)
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="PostgresEngineDataStoreImplementation"/> class.
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
        public PostgresEngineDataStore(string connectionString, int retryLimit, TimeSpan retryDelay) : base(retryLimit, retryDelay)
        {
            this.Store = new PostgresEngineDataStoreImplementation(connectionString);
        }


        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            IDisposable disposable = (this.Store as IDisposable);

            if (disposable != null)
            {
                disposable.Dispose();

                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources asynchronously.
        /// </summary>
        /// <returns>
        ///   A <see cref="ValueTask"/> value that represents the asynchronous dispose operation.
        /// </returns>
        public ValueTask DisposeAsync()
        {
            IAsyncDisposable disposable = (this.Store as IAsyncDisposable);
            ValueTask result = ValueTask.CompletedTask;

            if (disposable != null)
            {
                result = disposable.DisposeAsync();

                GC.SuppressFinalize(this);
            }

            return result;
        }
    }
}
