
using System.Runtime.Serialization;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   The exception that is thrown when the job being inserted already exists.
    /// </summary>
    [Serializable]
    public class JobAlreadyExistsException : Exception
    {
        /// <summary>
        ///   Initializes a new <see cref="JobAlreadyExistsException"/> class instance.
        /// </summary>
        public JobAlreadyExistsException() : this(null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="JobAlreadyExistsException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        public JobAlreadyExistsException(string message) : this(message, null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="JobAlreadyExistsException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        /// <param name="innerException">
        ///   The exception that is the cause of the current exception.
        /// </param>
        public JobAlreadyExistsException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="JobAlreadyExistsException"/> class with the specified serialization
        ///   and context information.
        /// </summary>
        /// <param name="info">
        ///   The data for serializing or deserializing the file.
        /// </param>
        /// <param name="context">
        ///   The source and destination for the file.
        /// </param>
        /// <remarks>
        protected JobAlreadyExistsException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
