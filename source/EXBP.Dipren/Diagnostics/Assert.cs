
using System.Diagnostics.CodeAnalysis;


namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Provides static methods for validating method arguments and object state.
    /// </summary>
    public static class Assert
    {
        /// <summary>
        ///   Throws an <see cref="ArgumentNullException"/> if the specified value is <c>null</c>.
        /// </summary>
        /// <param name="value">
        ///   The parameter value to verify.
        /// </param>
        /// <param name="name">
        ///   The name of the parameter.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <returns>
        ///   The argument <paramref name="value"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   The specified <paramref name="value"/> is <c>null</c>.
        /// </exception>
        [return: NotNullIfNotNull("value")]
        public static TValue ArgumentIsNotNull<TValue>(TValue value, string name, string message = null)
        {
            if (value == null)
            {
                Exception exception;

                if (message == null)
                {
                    exception = new ArgumentNullException(name);
                }
                else
                {
                    exception = new ArgumentNullException(name, message);
                }

                throw exception;
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified condition is <b>false</b>.
        /// </summary>
        /// <param name="condition">
        ///   The condition to evaluate.
        /// </param>
        /// <param name="name">
        ///   The name of the parameter.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <exception cref="ArgumentException">
        ///   The specified <paramref name="condition"/> is <b>false</b>.
        /// </exception>
        public static void ArgumentIsValid(bool condition, string name, string message = null)
        {
            if (condition == false)
            {
                Exception exception;

                if (message == null)
                {
                    exception = new ArgumentException(name);
                }
                else
                {
                    exception = new ArgumentException(message, name);
                }

                throw exception;
            }
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is an empty <see cref="string"/>.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="string"/> parameter value to verify.
        /// </param>
        /// <param name="name">
        ///   The name of the parameter.
        /// </param>
        /// <param name="whitespace">
        ///   <b>true</b> to treat strings with whitespace characters only as valid; otherwise, <b>false</b>.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <returns>
        ///   The argument <paramref name="value"/>.
        /// </returns>
        /// <exception cref="ArgumentException">
        ///   The specified <paramref name="value"/> is empty.
        /// </exception>
        /// <remarks>
        ///   This method does not throw an <see cref="ArgumentNullException"/> if <paramref name="value"/> is
        ///   <c>null</c>.
        /// </remarks>
        public static string ArgumentIsNotEmpty(string value, bool whitespace, string name, string message = null)
        {
            if ((null != value) && (((whitespace == true) && (value.Length == 0)) || ((whitespace == false) && (string.IsNullOrWhiteSpace(value) == true))))
            {
                throw new ArgumentException((message ?? AssertResources.MessageStringIsEmpty), name);
            }

            return value;
        }
    }
}
