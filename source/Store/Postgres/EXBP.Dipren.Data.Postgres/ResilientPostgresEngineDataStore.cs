
namespace EXBP.Dipren.Data.Postgres
{
    /// <summary>
    ///   Implements an <see cref="IEngineDataStore"/> that uses Postgres SQL as its storage engine and uses a backoff
    ///   retry policy for resilience.
    /// </summary>
    /// <remarks>
    ///   The implementation assumes that the required schema and table structure is already deployed.
    /// </remarks>
    public class ResilientPostgresEngineDataStore : ResilientEngineDataStore<PostgresEngineDataStore>, IDisposable, IAsyncDisposable
    {
        private const int DEFAULT_RETRY_LIMIT = 16;
        private const int DEFAULT_RETRY_DELAY = 20;

        private static readonly TimeSpan DefaultRetryDelay = TimeSpan.FromMilliseconds(DEFAULT_RETRY_DELAY);


        /// <summary>
        ///   Gets the <see cref="IEngineDataStore"/> object to wrap.
        /// </summary>
        /// <value>
        ///   The <see cref="IEngineDataStore"/> object being wrapped.
        /// </value>
        protected override PostgresEngineDataStore Store { get; }


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
        public ResilientPostgresEngineDataStore(string connectionString, int retryLimit = DEFAULT_RETRY_LIMIT) : this(connectionString, retryLimit, DefaultRetryDelay)
        {
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
        public ResilientPostgresEngineDataStore(string connectionString, int retryLimit, TimeSpan retryDelay) : base(retryLimit, retryDelay)
        {
            this.Store = new PostgresEngineDataStore(connectionString);
        }


        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this.Store.Dispose();

            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources asynchronously.
        /// </summary>
        /// <returns>
        ///   A <see cref="ValueTask"/> value that represents the asynchronous dispose operation.
        /// </returns>
        public ValueTask DisposeAsync()
        {
            ValueTask result = this.Store.DisposeAsync();

            GC.SuppressFinalize(this);

            return result;
        }
    }
}
