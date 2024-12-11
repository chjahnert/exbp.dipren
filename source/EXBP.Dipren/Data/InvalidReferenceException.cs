
namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   The exception that is thrown when an entry references cannot be resolved.
    /// </summary>
    [Serializable]
    public class InvalidReferenceException : Exception
    {
        /// <summary>
        ///   Initializes a new <see cref="InvalidReferenceException"/> class instance.
        /// </summary>
        public InvalidReferenceException() : this(null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="InvalidReferenceException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        public InvalidReferenceException(string message) : this(message, null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="InvalidReferenceException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        /// <param name="innerException">
        ///   The exception that is the cause of the current exception.
        /// </param>
        public InvalidReferenceException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
