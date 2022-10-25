
using System.Runtime.Serialization;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   The exception that is thrown when the lock related error occurs.
    /// </summary>
    [Serializable]
    public class LockException : Exception
    {
        /// <summary>
        ///   Initializes a new <see cref="LockException"/> class instance.
        /// </summary>
        public LockException() : this(null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="LockException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        public LockException(string message) : this(message, null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="LockException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        /// <param name="innerException">
        ///   The exception that is the cause of the current exception.
        /// </param>
        public LockException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="LockException"/> class with the specified serialization
        ///   and context information.
        /// </summary>
        /// <param name="info">
        ///   The data for serializing or deserializing the file.
        /// </param>
        /// <param name="context">
        ///   The source and destination for the file.
        /// </param>
        /// <remarks>
        protected LockException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
