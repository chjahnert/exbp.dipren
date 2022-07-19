﻿
namespace EXBP.Dipren
{
    /// <summary>
    ///   Allows a class to implement a data store for the distributed processing engine.
    /// </summary>
    public interface IEngineDataStore
    {
        /// <summary>
        ///   Returns the number of distributed processing jobs in the current data store.
        /// </summary>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> or <see cref="long"/> that represents the asynchronous operation and  can
        ///   be used to access the result.
        /// </returns>
        Task<long> CountJobsAsync(CancellationToken cancellation);

        /// <summary>
        ///   Inserts a job with the specified details into the current data store.
        /// </summary>
        /// <param name="name">
        ///   The unique name of the distributed processing job.
        /// </param>
        /// <param name="state">
        ///   The state of the new distributed processing job.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation.
        /// </returns>
        Task InsertJobAsync(string name, JobState state, CancellationToken cancellation);

        /// <summary>
        ///   Sets the state of a job to the specified value.
        /// </summary>
        /// <param name="name">
        ///   The unique name of the job to update.
        /// </param>
        /// <param name="state">
        ///   The new state to set.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation.
        /// </returns>
        Task SetJobStateAsync(string name, JobState state, CancellationToken cancellation);

        /// <summary>
        ///   Returns the current state of a job.
        /// </summary>
        /// <param name="name">
        ///   The unique name of the job.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="JobState"/> object that represents the asynchronous operation
        ///   and can be used to access the result.
        /// </returns>
        Task<JobState> GetJobStateAsync(string name, CancellationToken cancellation);
    }
}
