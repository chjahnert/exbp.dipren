
namespace EXBP.Dipren
{
    /// <summary>
    ///   Enumerates the states of a distributed processing job.
    /// </summary>
    public enum JobState
    {
        /// <summary>
        ///   The distributed processing job is being initialized.
        /// </summary>
        Initializing = 0,

        /// <summary>
        ///   The distributed processing is ready to be started.
        /// </summary>
        Ready = 1,

        /// <summary>
        ///   The distributed processing job is started and is now in progress.
        /// </summary>
        Processing = 2,

        /// <summary>
        ///   The distributed processing job is complete.
        /// </summary>
        Completed = 9
    }
}
