
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a retry policy that waits some time before retry attempts.
    /// </summary>
    internal class BackoffRetryPolicy
    {
        private readonly int _attempts;
        private readonly Func<int, TimeSpan> _getRetryDelayFunction;
        private readonly Func<Exception, bool> _isTransientErrorFunction;


        /// <summary>
        ///   Initializes a new instance of the <see cref="BackoffRetryPolicy"/> class.
        /// </summary>
        /// <param name="retryAttempts">
        ///   The total number of retry attempts to make.
        /// </param>
        /// <param name="getRetryDelay">
        ///   A function that returns the time to wait before retry attempts.
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
        public BackoffRetryPolicy(int retryAttempts, Func<int, TimeSpan> getRetryDelay, Func<Exception, bool> isTransientError)
        {
            Assert.ArgumentIsGreaterOrEqual(retryAttempts, 0, nameof(retryAttempts));
            Assert.ArgumentIsNotNull(getRetryDelay, nameof(getRetryDelay));
            Assert.ArgumentIsNotNull(isTransientError, nameof(isTransientError));

            this._attempts = (1 + retryAttempts);
            this._getRetryDelayFunction = getRetryDelay;
            this._isTransientErrorFunction = isTransientError;
        }


        /// <summary>
        ///   Executes the specified action. If the action fails with a transient error, it is retried up to the
        ///   number of times at construction time.
        /// </summary>
        /// <param name="action">
        ///   The action to be executed.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="action"/> is a <see langword="null"/> reference.
        /// </exception>
        public void Execute(Action action)
        {
            Assert.ArgumentIsNotNull(action, nameof(action));

            bool retry = false;
            int attempt = 0;

            do
            {
                retry = false;
                attempt += 1;

                try
                {
                    action.Invoke();
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
                                Thread.Sleep(duration);
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
        }

        /// <summary>
        ///   Executes the specified action. If the action fails with a transient error, it is retried up to the
        ///   number of times at construction time.
        /// </summary>
        /// <param name="action">
        ///   The action to be executed.
        /// </param>
        /// <param name="cancellation">
        ///   A <see cref="CancellationToken"/> value that can be used to cancel the operation.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="action"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <remarks>
        ///   The specified action is executed in a synchronous fashion, but the waiting between retry attempt is
        ///   asynchronous.
        /// </remarks>
        public async Task ExecuteAsync(Action action, CancellationToken cancellation = default)
        {
            Assert.ArgumentIsNotNull(action, nameof(action));

            bool retry = false;
            int attempt = 0;

            do
            {
                retry = false;
                attempt += 1;

                try
                {
                    action.Invoke();
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
        }

        /// <summary>
        ///   Executes the specified asynchronous action. If the action fails with a transient error, it is retried up
        ///   to the number of times at construction time.
        /// </summary>
        /// <param name="action">
        ///   The asynchronous action to execute.
        /// </param>
        /// <param name="cancellation">
        ///   A <see cref="CancellationToken"/> value that can be used to cancel the operation.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="action"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <remarks>
        ///   Both the specified action and the waiting between retry attempt is run in an asynchronous fashion.
        /// </remarks>
        public async Task ExecuteAsync(Func<Task> action, CancellationToken cancellation = default)
        {
            Assert.ArgumentIsNotNull(action, nameof(action));

            bool retry = false;
            int attempt = 0;

            do
            {
                retry = false;
                attempt += 1;

                try
                {
                    await action.Invoke();
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
        }

        /// <summary>
        ///   Executes the specified asynchronous function. If the function fails with a transient error, it is retried
        ///   up to the number of times at construction time.
        /// </summary>
        /// <typeparam name="TResult">
        ///   The type the asynchronous function returns.
        /// </typeparam>
        /// <param name="function">
        ///   The asynchronous function to execute.
        /// </param>
        /// <param name="cancellation">
        ///   A <see cref="CancellationToken"/> value that can be used to cancel the operation.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> object that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="function"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <remarks>
        ///   Both the specified action and the waiting between retry attempt is run in an asynchronous fashion.
        /// </remarks>
        public async Task<TResult> ExecuteAsync<TResult>(Func<Task<TResult>> function, CancellationToken cancellation = default)
        {
            Assert.ArgumentIsNotNull(function, nameof(function));

            bool retry = false;
            int attempt = 0;
            TResult result = default;

            do
            {
                retry = false;
                attempt += 1;

                try
                {
                    result = await function.Invoke();
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
    }
}
