
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a retry strategy that waits some time before each retry attempt.
    /// </summary>
    public class BackoffRetryStrategy : IAsyncRetryStrategy
    {
        private readonly int _attempts;
        private readonly Func<int, TimeSpan> _getRetryDelayFunction;
        private readonly Func<Exception, bool> _isTransientErrorFunction;


        /// <summary>
        ///   Initializes a new instance of the <see cref="BackoffRetryStrategy"/> class.
        /// </summary>
        /// <param name="retryAttempts">
        ///   The number of retry attempts to make in case the initial attempt fails due to a transient error
        ///   condition.
        /// </param>
        /// <param name="getRetryDelay">
        ///   A function that returns the time to wait before each retry attempt.
        /// </param>
        /// <param name="isTransientError">
        ///   A function that determines whether an exception represents a transient error condition.
        /// </param>
        /// <exception cref="ArgumentException">
        ///   Argument <paramref name="retryAttempts"/> is less than zero.
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="getRetryDelay"/> or <paramref name="isTransientError"/> is a
        ///   <see langword="null"/> reference.
        /// </exception>
        public BackoffRetryStrategy(int retryAttempts, Func<int, TimeSpan> getRetryDelay, Func<Exception, bool> isTransientError)
        {
            Assert.ArgumentIsGreaterOrEqual(retryAttempts, 0, nameof(retryAttempts));
            Assert.ArgumentIsNotNull(getRetryDelay, nameof(getRetryDelay));
            Assert.ArgumentIsNotNull(isTransientError, nameof(isTransientError));

            this._attempts = (1 + retryAttempts);
            this._getRetryDelayFunction = getRetryDelay;
            this._isTransientErrorFunction = isTransientError;
        }


        /// <summary>
        ///   Executes the specified action with the current retry strategy.
        /// </summary>
        /// <param name="action">
        ///   The action to execute.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="action"/> is a <see langword="null"/> reference.
        /// </exception>
        public async Task ExecuteAsync(Func<Task> action, CancellationToken cancellation = default)
        {
            Assert.ArgumentIsNotNull(action, nameof(action));

            Func<Task<Empty>> function = async () =>
            {
                await action();
                return Empty.Value;
            };

            await this.ExecuteAsync(function, cancellation);
        }


        /// <summary>
        ///   Executes the specified action with the current retry strategy.
        /// </summary>
        /// <typeparam name="TResult">
        ///   The type of the result returned by the asynchronous operation.
        /// </typeparam>
        /// <param name="action">
        ///   The action to execute.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> object that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="action"/> is a <see langword="null"/> reference.
        /// </exception>
        public async Task<TResult> ExecuteAsync<TResult>(Func<Task<TResult>> action, CancellationToken cancellation = default)
        {
            Assert.ArgumentIsNotNull(action, nameof(action));

            bool retry = false;
            int attempt = 0;
            TResult result = default;

            do
            {
                cancellation.ThrowIfCancellationRequested();

                retry = false;
                attempt += 1;

                try
                {
                    result = await action.Invoke();
                }
                catch (Exception exception)
                {
                    if (attempt < this._attempts)
                    {
                        retry = this._isTransientErrorFunction.Invoke(exception);

                        if (retry == true)
                        {
                            TimeSpan duration = this._getRetryDelayFunction.Invoke(attempt);

                            if (duration >= TimeSpan.Zero)
                            {
                                await Task.Delay(duration, cancellation);
                            }
                        }
                        else
                        {
                            throw;
                        }
                    }
                    else
                    {
                        throw;
                    }
                }
            }
            while (retry == true);

            return result;
        }


        /// <summary>
        ///   An empty structure used as a return value when wrapping asynchronous operations into asynchronous
        ///   function calls.
        /// </summary>
        private struct Empty
        {
            /// <summary>
            ///   Gets an instance of the <see cref="Empty"/> type.
            /// </summary>
            /// <value>
            ///   An instance of the <see cref="Empty"/> type.
            /// </value>
            public static Empty Value { get; } = new Empty();
        };
    }
}
