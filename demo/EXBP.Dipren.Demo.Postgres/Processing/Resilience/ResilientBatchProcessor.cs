
using System.Diagnostics;

using EXBP.Dipren.Resilience;


namespace EXBP.Dipren.Demo.Postgres.Processing.Resilience
{
    internal class ResilientBatchProcessor<TItem> : IBatchProcessor<TItem>
    {
        private const int RETRY_LIMIT = 8;
        private const int RETRY_BACKOFF_DELAY_MS = 25;


        private readonly IBatchProcessor<TItem> _batchProcessor;
        private readonly IAsyncRetryStrategy _retryStrategy;


        internal ResilientBatchProcessor(IBatchProcessor<TItem> batchProcessor)
        {
            Debug.Assert(batchProcessor != null);

            TimeSpan backoffDelay = TimeSpan.FromMilliseconds(RETRY_BACKOFF_DELAY_MS);
            IBackoffDelayProvider delayProvider = new PresetBackoffDelayProvider(backoffDelay);

            _retryStrategy = new BackoffRetryStrategy(RETRY_LIMIT, delayProvider, DbTransientErrorDetector.Default);
            _batchProcessor = batchProcessor;
        }


        public async Task ProcessAsync(IEnumerable<TItem> items, CancellationToken cancellation)
            => await _retryStrategy.ExecuteAsync(async () => await _batchProcessor.ProcessAsync(items, cancellation));
    }
}
