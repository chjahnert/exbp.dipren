
using NUnit.Framework;

using EXBP.Dipren.Resilience;


namespace EXBP.Dipren.Tests.Resilience
{
    [TestFixture]
    public class BackoffRetryStrategyTests
    {
        [Test]
        public async Task Execute_PermanentErrorOccurrs_NoRetryIsAttempted()
        {
            BackoffRetryPolicy policy = new BackoffRetryPolicy(1, (attempt, exception) => TimeSpan.Zero, (attempt, exception) => false);

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
        public void Execute_TransientErrorOccurrs_RetryIsAttempted()
        {
            BackoffRetryPolicy policy = new BackoffRetryPolicy(3, (attempt, exception) => TimeSpan.Zero, (attempt, exception) => true);

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
        public async Task Execute_TransientErrorOccurrs_StopsAfterSuccessfulAttempts()
        {
            BackoffRetryPolicy policy = new BackoffRetryPolicy(3, (attempt, exception) => TimeSpan.Zero, (attempt, exception) => true);

            int executions = 0;

            int attempts = await policy.ExecuteAsync(() => {

               executions += 1;

               if (executions < 2)
               {
                   throw new Exception("Transient error condition.");
               }
            });

            Assert.That(attempts, Is.EqualTo(2));
        }
    }
}
