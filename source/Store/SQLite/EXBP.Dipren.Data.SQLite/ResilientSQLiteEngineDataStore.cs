
namespace EXBP.Dipren.Data.SQLite
{
    /// <summary>
    ///   Implements an <see cref="IEngineDataStore"/> that uses SQLite as its storage engine as its storage engine and
    ///   uses a backoff retry policy for resilience.
    /// </summary>
    public class ResilientSQLiteEngineDataStore : ResilientEngineDataStore<SQLiteEngineDataStore>, IDisposable, IAsyncDisposable
    {
        private const int DEFAULT_RETRY_LIMIT = 16;
        private const int DEFAULT_RETRY_DELAY = 20;

        private static readonly TimeSpan DefaultRetryDelay = TimeSpan.FromMilliseconds(DEFAULT_RETRY_DELAY);


        /// <summary>
        ///   Gets the <see cref="SQLiteEngineDataStore"/> object being wrap.
        /// </summary>
        /// <value>
        ///   The <see cref="SQLiteEngineDataStore"/> object being wrapped.
        /// </value>
        protected override SQLiteEngineDataStore Store { get; }


        /// <summary>
        ///   Initializes a new instance of the <see cref="ResilientSQLiteEngineDataStore"/> class.
        /// </summary>
        /// <param name="connectionString">
        ///   The connection string to use when connecting to the database.
        /// </param>
        /// <param name="retryLimit">
        ///   The number of retry attempts to perform in case a transient error occurs. By default, operations are
        ///   retried up to 16 times.
        /// </param>
        public ResilientSQLiteEngineDataStore(string connectionString, int retryLimit = DEFAULT_RETRY_LIMIT) : this(connectionString, retryLimit, DefaultRetryDelay)
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="ResilientSQLiteEngineDataStore"/> class.
        /// </summary>
        /// <param name="connectionString">
        ///   The connection string to use when connecting to the database.
        /// </param>
        /// <param name="retryLimit">
        ///   The number of retry attempts to perform in case a transient error occurs.
        /// </param>
        /// <param name="retryDelay">
        ///   
        /// </param>
        public ResilientSQLiteEngineDataStore(string connectionString, int retryLimit, TimeSpan retryDelay) : base(retryLimit, retryDelay)
        {
            this.Store = new SQLiteEngineDataStore(connectionString);
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
