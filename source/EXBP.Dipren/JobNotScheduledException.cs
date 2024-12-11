
namespace EXBP.Dipren
{
    /// <summary>
    ///   The exception that is thrown when the job being started that was not scheduled.
    /// </summary>
    [Serializable]
    public class JobNotScheduledException : Exception
    {
        private readonly string _id;


        /// <summary>
        ///   Gets the unique identifier of the job.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the unique identifier of the job.
        /// </value>
        public string Id => this._id;


        /// <summary>
        ///   Initializes a new <see cref="JobNotScheduledException"/> class instance.
        /// </summary>
        public JobNotScheduledException() : this(null, null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="JobNotScheduledException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        /// <param name="id">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        public JobNotScheduledException(string message, string id) : this(message, id, null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="JobNotScheduledException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        /// <param name="id">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        /// <param name="innerException">
        ///   The exception that is the cause of the current exception.
        /// </param>
        public JobNotScheduledException(string message, string id, Exception innerException) : base(message, innerException)
        {
            this._id = id;
        }
    }
}
