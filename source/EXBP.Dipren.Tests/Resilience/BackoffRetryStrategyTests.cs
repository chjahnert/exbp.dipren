
using NUnit.Framework;

using EXBP.Dipren.Resilience;


namespace EXBP.Dipren.Tests.Resilience
{
    [TestFixture]
    public class BackoffRetryStrategyTests
    {
        [Test]
        public void Execute_ArgumentActionIsNull_ThrowsException()
        {
            BackoffRetryStrategy policy = new BackoffRetryStrategy(1, attempt => TimeSpan.Zero, exception => false);

            Action action = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => policy.ExecuteAsync(action));
        }

        [Test]
        public void ExecuteAsync_ArgumentActionIsNull_ThrowsException()
        {
            BackoffRetryStrategy policy = new BackoffRetryStrategy(1, attempt => TimeSpan.Zero, exception => false);

            Func<Task> action = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => policy.ExecuteAsync(action));
        }

        [Test]
        public void ExecuteAsync_ArgumentFunctionIsNull_ThrowsException()
        {
            BackoffRetryStrategy policy = new BackoffRetryStrategy(1, attempt => TimeSpan.Zero, exception => false);

            Func<Task<int>> function = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => policy.ExecuteAsync<int>(function));
        }

        [Test]
        public async Task ExecuteAsync_PermanentErrorOccurrs_NoRetryIsAttempted()
        {
            BackoffRetryStrategy policy = new BackoffRetryStrategy(1, attempt => TimeSpan.Zero, exception => false);

            int attempts = 0;
            int thrown = 0;

            try
            {
                await policy.ExecuteAsync(() => {
                   attempts += 1;
                   throw new Exception("Permanent error condition.");
                });
            }
            catch
            {
                thrown += 1;
            }

            Assert.That(attempts, Is.EqualTo(1));
            Assert.That(thrown, Is.EqualTo(1));
        }

        [Test]
        public void ExecuteAsync_TransientErrorOccurrsDuringAction_RetryIsAttempted()
        {
            BackoffRetryStrategy policy = new BackoffRetryStrategy(3, attempt => TimeSpan.Zero, exception => true);

            int attempts = 0;

            Assert.ThrowsAsync<Exception>(async () => {
                await policy.ExecuteAsync(() => {
                    attempts += 1;
                    throw new Exception("Transient error condition.");
                });
            });

            Assert.That(attempts, Is.EqualTo(4));
        }

        [Test]
        public void ExecuteAsync_TransientErrorOccurrsDuringFunction_RetryIsAttempted()
        {
            BackoffRetryStrategy policy = new BackoffRetryStrategy(3, attempt => TimeSpan.Zero, exception => true);

            int attempts = 0;

            Func<Task<int>> function = () => {
                attempts += 1;
                throw new Exception("Transient error condition.");
            };

            Assert.ThrowsAsync<Exception>(async () => {
                await policy.ExecuteAsync(function);
            });

            Assert.That(attempts, Is.EqualTo(4));
        }

        [Test]
        public void Execute_TransientErrorOccurrsDuringAction_StopsAfterSuccessfulAttempts()
        {
            BackoffRetryStrategy policy = new BackoffRetryStrategy(3, attempt => TimeSpan.Zero, exception => true);

            int executions = 0;

            policy.Execute(() => {

               executions += 1;

               if (executions < 2)
               {
                   throw new Exception("Transient error condition.");
               }
            });

            Assert.That(executions, Is.EqualTo(2));
        }

        [Test]
        public async Task ExecuteAsync_TransientErrorOccurrsDuringAction_StopsAfterSuccessfulAttempts()
        {
            BackoffRetryStrategy policy = new BackoffRetryStrategy(3, attempt => TimeSpan.Zero, exception => true);

            int executions = 0;

            await policy.ExecuteAsync(() => {

               executions += 1;

               if (executions < 2)
               {
                   throw new Exception("Transient error condition.");
               }
            });

            Assert.That(executions, Is.EqualTo(2));
        }

        [Test]
        public async Task ExecuteAsync_TransientErrorOccurrsDuringFunction_StopsAfterSuccessfulAttempts()
        {
            BackoffRetryStrategy policy = new BackoffRetryStrategy(3, attempt => TimeSpan.Zero, exception => true);

            int executions = 0;

            await policy.ExecuteAsync(() => {

               executions += 1;

               if (executions < 2)
               {
                   throw new Exception("Transient error condition.");
               }

                return Task.FromResult(1);
            });

            Assert.That(executions, Is.EqualTo(2));
        }
    }
}
