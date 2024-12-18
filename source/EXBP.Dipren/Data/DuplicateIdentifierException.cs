﻿
namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   The exception that is thrown when the job being inserted already exists.
    /// </summary>
    [Serializable]
    public class DuplicateIdentifierException : Exception
    {
        /// <summary>
        ///   Initializes a new <see cref="DuplicateIdentifierException"/> class instance.
        /// </summary>
        public DuplicateIdentifierException() : this(null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="DuplicateIdentifierException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        public DuplicateIdentifierException(string message) : this(message, null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="DuplicateIdentifierException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        /// <param name="innerException">
        ///   The exception that is the cause of the current exception.
        /// </param>
        public DuplicateIdentifierException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
