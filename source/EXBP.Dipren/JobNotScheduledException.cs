
using System.Runtime.Serialization;


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
        public JobNotScheduledException(string message, string id) : this(message, id, null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="JobNotScheduledException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        /// <param name="innerException">
        ///   The exception that is the cause of the current exception.
        /// </param>
        public JobNotScheduledException(string message, string id, Exception innerException) : base(message, innerException)
        {
            this._id = id;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="JobNotScheduledException"/> class with the specified serialization
        ///   and context information.
        /// </summary>
        /// <param name="info">
        ///   The data for serializing or deserializing the file.
        /// </param>
        /// <param name="context">
        ///   The source and destination for the file.
        /// </param>
        /// <remarks>
        protected JobNotScheduledException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this._id = info.GetString(nameof(JobNotScheduledException) + "." + nameof(this._id));
        }


        /// <summary>
        ///   When overridden in a derived class, sets the <see cref="SerializationInfo"/> with information about the
        ///   exception.
        /// </summary>
        /// <param name="info">
        ///   The <see cref="SerializationInfo"/> that holds the serialized object data about the exception being
        ///   thrown.
        /// </param>
        /// <param name="context">
        ///   The <see cref="StreamingContext"/> that contains contextual information about the source or destination.
        /// </param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            info.AddValue(nameof(JobNotScheduledException) + "." + nameof(this._id), this._id);
        }
    }
}
