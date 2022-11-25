
using System.Diagnostics;

using EXBP.Dipren.Resilience;


namespace EXBP.Dipren.Demo.Postgres.Processing.Resilience
{
    internal class ResilientDataSource<TKey, TItem> : IDataSource<TKey, TItem>
    {
        private const int RETRY_LIMIT = 8;
        private const int RETRY_BACKOFF_DELAY_MS = 25;

        private readonly IDataSource<TKey, TItem> _dataSource;
        private readonly IAsyncRetryStrategy _retrier;


        internal ResilientDataSource(IDataSource<TKey, TItem> dataSource)
        {
            Debug.Assert(dataSource != null);

            TimeSpan backoffDelay = TimeSpan.FromMilliseconds(RETRY_BACKOFF_DELAY_MS);
            IBackoffDelayProvider delayProvider = new PresetBackoffDelayProvider(backoffDelay);

            _retrier = new BackoffRetryStrategy(RETRY_LIMIT, delayProvider, DbTransientErrorDetector.Default);
            _dataSource = dataSource;
        }


        public async Task<long> EstimateRangeSizeAsync(Range<TKey> range, CancellationToken cancellation)
            => await _retrier.ExecuteAsync(async () => await _dataSource.EstimateRangeSizeAsync(range, cancellation), cancellation);

        public async Task<Range<TKey>> GetEntireRangeAsync(CancellationToken cancellation)
            => await _retrier.ExecuteAsync(async () => await _dataSource.GetEntireRangeAsync(cancellation), cancellation);

        public async Task<IEnumerable<KeyValuePair<TKey, TItem>>> GetNextBatchAsync(Range<TKey> range, int skip, int take, CancellationToken cancellation)
            => await _retrier.ExecuteAsync(async () => await _dataSource.GetNextBatchAsync(range, skip, take, cancellation), cancellation);
    }
}
