
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a retry policy that is based on delays between retry attempts.
    /// </summary>
    public class BackoffRetryPolicy
    {
        private readonly int _attempts;
        private readonly Func<int, Exception, TimeSpan> _getRetryDelay;
        private readonly Func<int, Exception, bool> _isTransientError;


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
        public BackoffRetryPolicy(int retries, Func<int, Exception, TimeSpan> getRetryDelay, Func<int, Exception, bool> isTransientError)
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
        /// <returns>
        ///   An <see cref="int"/> value that indicates the total number of attempts performed.
        /// </returns>
        public int Execute(Action action)
        {
            Assert.ArgumentIsNotNull(action, nameof(action));

            bool retry = false;
            int result = 0;

            do
            {
                retry = false;
                result += 1;

                try
                {
                    action.Invoke();
                }
                catch (Exception exception)
                {
                    if (result < this._attempts)
                    {
                        retry = this._isTransientError.Invoke(result, exception);

                        if (retry == true)
                        {
                            TimeSpan duration = this._getRetryDelay.Invoke(result, exception);

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

            return result;
        }

        /// <summary>
        ///   Runs the specified action.
        /// </summary>
        /// <param name="action">
        ///   The action to be executed.
        /// </param>
        /// <returns>
        ///   An <see cref="int"/> value that indicates the total number of execution attempts performed.
        /// </returns>
        public async Task<int> ExecuteAsync(Action action) => await this.ExecuteAsync(action, CancellationToken.None);

        /// <summary>
        ///   Runs the specified action.
        /// </summary>
        /// <param name="action">
        ///   The action to be executed.
        /// </param>
        /// <param name="cancellation">
        ///   A <see cref="CancellationToken"/> value that can be used to cancel the operation.
        /// </param>
        /// <returns>
        ///   An <see cref="int"/> value that indicates the total number of execution attempts performed.
        /// </returns>
        public async Task<int> ExecuteAsync(Action action, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(action, nameof(action));

            bool retry = false;
            int result = 0;

            do
            {
                retry = false;
                result += 1;

                try
                {
                    action.Invoke();
                }
                catch (Exception exception)
                {
                    if (result < this._attempts)
                    {
                        retry = this._isTransientError.Invoke(result, exception);

                        if (retry == true)
                        {
                            TimeSpan duration = this._getRetryDelay.Invoke(result, exception);

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
        ///   Runs the specified asynchronous method.
        /// </summary>
        /// <param name="action">
        ///   The asynchronous action to be executed.
        /// </param>
        /// <returns>
        ///   An <see cref="int"/> value that indicates the total number of execution attempts performed.
        /// </returns>
        public async Task<int> ExecuteAsync(Func<Task> action) => await this.ExecuteAsync(action, CancellationToken.None);

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
        public async Task<int> ExecuteAsync(Func<Task> action, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(action, nameof(action));

            bool retry = false;
            int result = 0;

            do
            {
                retry = false;
                result += 1;

                try
                {
                    await action.Invoke();
                }
                catch (Exception exception)
                {
                    if (result < this._attempts)
                    {
                        retry = this._isTransientError.Invoke(result, exception);

                        if (retry == true)
                        {
                            TimeSpan duration = this._getRetryDelay.Invoke(result, exception);

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
