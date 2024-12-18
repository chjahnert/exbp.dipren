﻿
namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   The exception that is thrown when the entry being updated does not exists.
    /// </summary>
    [Serializable]
    public class UnknownIdentifierException : Exception
    {
        /// <summary>
        ///   Initializes a new <see cref="UnknownIdentifierException"/> class instance.
        /// </summary>
        public UnknownIdentifierException() : this(null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="UnknownIdentifierException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        public UnknownIdentifierException(string message) : this(message, null)
        {
        }

        /// <summary>
        ///   Initializes a new <see cref="UnknownIdentifierException"/> class instance.
        /// </summary>
        /// <param name="message">
        ///   A <see cref="string"/> that describes the error.
        /// </param>
        /// <param name="innerException">
        ///   The exception that is the cause of the current exception.
        /// </param>
        public UnknownIdentifierException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
