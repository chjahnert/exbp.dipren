
using Npgsql;


namespace EXBP.Dipren.Data.Postgres
{
    /// <summary>
    ///   Implements an <see cref="IEngineDataStore"/> that uses Postgres SQL as its storage engine and employs a
    ///   backoff retry policy for resilience.
    /// </summary>
    /// <remarks>
    ///   The implementation assumes that the required schema and table structure is already deployed.
    /// </remarks>
    public class PostgresResilientEngineDataStore : ResilientEngineDataStore, IDisposable, IAsyncDisposable
    {
        private const int DEFAULT_RETRY_LIMIT = 16;
        private const int DEFAULT_RETRY_DELAY = 20;

        private static readonly TimeSpan DefaultRetryDelay = TimeSpan.FromMilliseconds(DEFAULT_RETRY_DELAY);


        private readonly PostgresEngineDataStore _dataStore;


        /// <summary>
        ///   Gets the <see cref="IEngineDataStore"/> object to wrap.
        /// </summary>
        /// <value>
        ///   The <see cref="IEngineDataStore"/> object being wrapped.
        /// </value>
        protected override IEngineDataStore Store => this._dataStore;


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
        public PostgresResilientEngineDataStore(string connectionString, int retryLimit = DEFAULT_RETRY_LIMIT) : this(connectionString, retryLimit, DefaultRetryDelay)
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
        public PostgresResilientEngineDataStore(string connectionString, int retryLimit, TimeSpan retryDelay) : base(retryLimit, retryDelay)
        {
            this._dataStore = new PostgresEngineDataStore(connectionString);
        }


        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this._dataStore.Dispose();

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
            ValueTask result = this._dataStore.DisposeAsync();

            GC.SuppressFinalize(this);

            return result;
        }

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
        protected override bool IsTransientError(Exception exception)
            => (exception as PostgresException)?.IsTransient ?? false;
    }
}
