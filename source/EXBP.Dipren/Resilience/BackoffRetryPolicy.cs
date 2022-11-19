
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a retry policy that is based on delays between retry attempts.
    /// </summary>
    public class BackoffRetryPolicy
    {
        private readonly int _attempts;
        private readonly Func<int, TimeSpan> _getRetryDelay;
        private readonly Func<Exception, bool> _isTransientError;


        /// <summary>
        ///   Initializes a new instance of the <see cref="BackoffRetryPolicy"/> class.
        /// </summary>
        /// <param name="retries">
        ///   The total number of retry attempts to make before failing the operation.
        /// </param>
        /// <param name="getRetryDelay">
        ///   A <see cref="Func{T1, T2, TResult}"/> that returns the time to wait before retry attempts.
        /// </param>
        /// <param name="isTransientError">
        ///   A <see cref="Func{T1, T2, TResult}"/> object that determines whether an exception represents a transient
        ///   error condition.
        /// </param>
        /// <exception cref="ArgumentException">
        ///   Argument <
        /// </exception>
        public BackoffRetryPolicy(int retries, Func<int, TimeSpan> getRetryDelay, Func<Exception, bool> isTransientError)
        {
            Assert.ArgumentIsGreaterOrEqual(retries, 0, nameof(retries));
            Assert.ArgumentIsNotNull(getRetryDelay, nameof(getRetryDelay));
            Assert.ArgumentIsNotNull(isTransientError, nameof(isTransientError));

            this._attempts = (1 + retries);
            this._getRetryDelay = getRetryDelay;
            this._isTransientError = isTransientError;
        }


        /// <summary>
        ///   Executes the specified action.
        /// </summary>
        /// <param name="action">
        ///   The action to be executed.
        /// </param>
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
                        retry = this._isTransientError.Invoke(exception);

                        if (retry == true)
                        {
                            TimeSpan duration = this._getRetryDelay.Invoke(attempt);

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
        ///   Executes the specified action.
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
                        retry = this._isTransientError.Invoke(exception);

                        if (retry == true)
                        {
                            TimeSpan duration = this._getRetryDelay.Invoke(attempt);

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
        ///   Runs the specified asynchronous method.
        /// </summary>
        /// <param name="action">
        ///   The asynchronous action to be executed.
        /// </param>
        /// <param name="cancellation">
        ///   A <see cref="CancellationToken"/> value that can be used to cancel the operation.
        /// </param>
        /// <returns>
        ///   An <see cref="int"/> value that indicates the total number of execution attempts performed.
        /// </returns>
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
                        retry = this._isTransientError.Invoke(exception);

                        if (retry == true)
                        {
                            TimeSpan duration = this._getRetryDelay.Invoke(attempt);

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
        ///   Runs the specified asynchronous method.
        /// </summary>
        /// <param name="action">
        ///   The asynchronous action to be executed.
        /// </param>
        /// <param name="cancellation">
        ///   A <see cref="CancellationToken"/> value that can be used to cancel the operation.
        /// </param>
        /// <returns>
        ///   An <see cref="int"/> value that indicates the total number of execution attempts performed.
        /// </returns>
        public async Task<TResult> ExecuteAsync<TResult>(Func<Task<TResult>> action, CancellationToken cancellation = default)
        {
            Assert.ArgumentIsNotNull(action, nameof(action));

            bool retry = false;
            int attempt = 0;
            TResult result = default;

            do
            {
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
                        retry = this._isTransientError.Invoke(exception);

                        if (retry == true)
                        {
                            TimeSpan duration = this._getRetryDelay.Invoke(attempt);

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
