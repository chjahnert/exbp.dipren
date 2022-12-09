
using System.Collections;
using System.Diagnostics.CodeAnalysis;


namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Provides static methods for validating method arguments and object state.
    /// </summary>
    public static class Assert
    {
        /// <summary>
        ///   Throws an <see cref="ArgumentNullException"/> if the specified value is <see langword="null"/>.
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
        ///   The specified <paramref name="value"/> is <see langword="null"/>.
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
        ///   Throws an <see cref="ArgumentException"/> if the specified condition is <see langword="false"/>.
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
        ///   The specified <paramref name="condition"/> is <see langword="false"/>.
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
        ///   <see langword="true"/> to treat strings with whitespace characters only as valid; otherwise, <see langword="false"/>.
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
        ///   <see langword="null"/>.
        /// </remarks>
        public static string ArgumentIsNotEmpty(string value, bool whitespace, string name, string message = null)
        {
            if ((null != value) && (((whitespace == true) && (value.Length == 0)) || ((whitespace == false) && (string.IsNullOrWhiteSpace(value) == true))))
            {
                throw new ArgumentException((message ?? AssertResources.MessageStringIsEmpty), name);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified collection has no elements.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="IEnumerable{T}"/> value to verify.
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
        /// <exception cref="ArgumentException">
        ///   Argument <paramref name="value"/> has no elements.
        /// </exception>
        /// <remarks>
        ///   This method does not throw an <see cref="ArgumentNullException"/> if <paramref name="value"/> is
        ///   <see langword="null"/>.
        /// </remarks>
        public static TValue ArgumentIsNotEmpty<TValue>(TValue value, string name, string message = null) where TValue : IEnumerable
        {
            if (null != value)
            {
                IEnumerator enumerator = value.GetEnumerator();
                bool exists = enumerator.MoveNext();

                if (exists == false)
                {
                    throw new ArgumentException((message ?? AssertResources.MessageCollectionIsEmpty), name);
                }
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is an empty <see cref="Guid"/>.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="Guid"/> parameter value to verify.
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
        /// <exception cref="ArgumentException">
        ///   The specified <paramref name="value"/> is empty.
        /// </exception>
        public static Guid ArgumentIsNotEmpty(Guid value, string name, string message = null)
        {
            if (value == Guid.Empty)
            {
                throw new ArgumentException((message ?? AssertResources.MessageGuidIsEmpty), name);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is not defined by the enumeration.
        /// </summary>
        /// <typeparam name="TEnum">
        ///   The type of the enumeration to check.
        /// </typeparam>
        /// <param name="value">
        ///   The value to verify.
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
        /// <exception cref="ArgumentException">
        ///   <paramref name="value"/> is not defined in the specified enumeration; or <typeparamref name="TEnum"/> is
        ///   not an enumeration, or type of <paramref name="value"/> is not the underlying type of the  enumeration.
        /// </exception>
        public static TEnum ArgumentIsDefined<TEnum>(TEnum value, string name, string message = null) where TEnum : struct => Assert.ArgumentIsDefined<TEnum, TEnum>(value, name, message);

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is not defined by the enumeration.
        /// </summary>
        /// <typeparam name="TEnum">
        ///   The type of the enumeration to check.
        /// </typeparam>
        /// <param name="value">
        ///   The value to verify.
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
        /// <exception cref="ArgumentException">
        ///   <paramref name="value"/> is not defined in the specified enumeration; or <typeparamref name="TEnum"/> is
        ///   not an enumeration, or type of <paramref name="value"/> is not the underlying type of the  enumeration.
        /// </exception>
        public static string ArgumentIsDefined<TEnum>(string value, string name, string message = null) where TEnum : struct => Assert.ArgumentIsDefined<string, TEnum>(value, name, message);

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is not defined by the enumeration.
        /// </summary>
        /// <typeparam name="TEnum">
        ///   The type of the enumeration to check.
        /// </typeparam>
        /// <param name="value">
        ///   The value to verify.
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
        /// <exception cref="ArgumentException">
        ///   <paramref name="value"/> is not defined in the specified enumeration; or <typeparamref name="TEnum"/> is
        ///   not an enumeration, or type of <paramref name="value"/> is not the underlying type of the  enumeration.
        /// </exception>
        public static byte ArgumentIsDefined<TEnum>(byte value, string name, string message = null) where TEnum : struct => Assert.ArgumentIsDefined<byte, TEnum>(value, name, message);

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is not defined by the enumeration.
        /// </summary>
        /// <typeparam name="TEnum">
        ///   The type of the enumeration to check.
        /// </typeparam>
        /// <param name="value">
        ///   The value to verify.
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
        /// <exception cref="ArgumentException">
        ///   <paramref name="value"/> is not defined in the specified enumeration; or <typeparamref name="TEnum"/> is
        ///   not an enumeration, or type of <paramref name="value"/> is not the underlying type of the  enumeration.
        /// </exception>
        public static short ArgumentIsDefined<TEnum>(short value, string name, string message = null) where TEnum : struct => Assert.ArgumentIsDefined<short, TEnum>(value, name, message);

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is not defined by the enumeration.
        /// </summary>
        /// <typeparam name="TEnum">
        ///   The type of the enumeration to check.
        /// </typeparam>
        /// <param name="value">
        ///   The value to verify.
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
        /// <exception cref="ArgumentException">
        ///   <paramref name="value"/> is not defined in the specified enumeration; or <typeparamref name="TEnum"/> is
        ///   not an enumeration, or type of <paramref name="value"/> is not the underlying type of the  enumeration.
        /// </exception>
        public static int ArgumentIsDefined<TEnum>(int value, string name, string message = null) where TEnum : struct => Assert.ArgumentIsDefined<int, TEnum>(value, name, message);

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is not defined by the enumeration.
        /// </summary>
        /// <typeparam name="TEnum">
        ///   The type of the enumeration to check.
        /// </typeparam>
        /// <param name="value">
        ///   The value to verify.
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
        /// <exception cref="ArgumentException">
        ///   <paramref name="value"/> is not defined in the specified enumeration; or <typeparamref name="TEnum"/> is
        ///   not an enumeration, or type of <paramref name="value"/> is not the underlying type of the  enumeration.
        /// </exception>
        public static long ArgumentIsDefined<TEnum>(long value, string name, string message = null) where TEnum : struct => Assert.ArgumentIsDefined<long, TEnum>(value, name, message);

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is not defined by the enumeration.
        /// </summary>
        /// <typeparam name="TValue">
        ///   The type of the value to verify.
        /// </typeparam>
        /// <typeparam name="TEnum">
        ///   The type of the enumeration to check.
        /// </typeparam>
        /// <param name="value">
        ///   The value to verify.
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
        /// <exception cref="ArgumentException">
        ///   <paramref name="value"/> is not defined in the specified enumeration; or <typeparamref name="TEnum"/> is
        ///   not an enumeration, or type of <paramref name="value"/> is not the underlying type of the  enumeration.
        /// </exception>
        private static TValue ArgumentIsDefined<TValue, TEnum>(TValue value, string name, string message = null)
        {
            Type type = typeof(TEnum);

            if (type.IsEnum == false)
            {
                throw new ArgumentException(AssertResources.MessageTypeIsNotEnum, nameof(value));
            }

            bool defined = ((value != null) ? Enum.IsDefined(type, value) : false);

            if (defined == false)
            {
                throw new ArgumentException(message, name);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is not a number.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="float"/> parameter value to verify.
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
        /// <exception cref="ArgumentException">
        ///   The specified <paramref name="value"/> is not a number.
        /// </exception>
        public static float ArgumentIsNumber(float value, string name, string message = null)
        {
            bool isNaN = float.IsNaN(value) || double.IsInfinity(value);

            if (isNaN == true)
            {
                throw new ArgumentException(message, name);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is not a number.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="double"/> parameter value to verify.
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
        /// <exception cref="ArgumentException">
        ///   The specified <paramref name="value"/> is not a number.
        /// </exception>
        public static double ArgumentIsNumber(double value, string name, string message = null)
        {
            bool isNaN = double.IsNaN(value) || double.IsInfinity(value);

            if (isNaN == true)
            {
                throw new ArgumentException(message, name);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified <see cref="DateTime"/> value is not specified
        ///   in local time.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="DateTime"/> value to verify.
        /// </param>
        /// <param name="name">
        ///   The name of the parameter.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <exception cref="ArgumentException">
        ///   The specified <paramref name="value"/> is not in local time.
        /// </exception>
        public static DateTime ArgumentIsLocalTime(DateTime value, string name, string message = null)
        {
            if (value.Kind != DateTimeKind.Local)
            {
                throw new ArgumentException((message ?? AssertResources.MessageDateTimeNotLocal), name);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified <see cref="DateTime"/> value is not specified
        ///   in UTC time.
        /// </summary>
        /// <param name="value">
        ///   The <see cref="DateTime"/> value to verify.
        /// </param>
        /// <param name="name">
        ///   The name of the parameter.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <exception cref="ArgumentException">
        ///   The specified <paramref name="value"/> is not in UTC time.
        /// </exception>
        public static DateTime ArgumentIsUniversalTime(DateTime value, string name, string message = null)
        {
            if (value.Kind != DateTimeKind.Utc)
            {
                throw new ArgumentException((message ?? AssertResources.MessageDateTimeNotUtc), name);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is less than or equal to the specified
        ///   comparand.
        /// </summary>
        /// <typeparam name="TValue">
        ///   A type that implements the <see cref="IComparable{T}"/> interface.
        /// </typeparam>
        /// <param name="value">
        ///   The parameter value to be tested.
        /// </param>
        /// <param name="comparand">
        ///   The value to compare against.
        /// </param>
        /// <param name="name">
        ///   The name of the parameter.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="comparand"/> is <see langword="null"/>.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///   Argument <paramref name="value"/> is less than or equal to <paramref name="comparand"/>.
        /// </exception>
        public static TValue ArgumentIsGreater<TValue>(TValue value, TValue comparand, string name, string message = null) where TValue : IComparable<TValue>
        {
            Assert.ArgumentIsNotNull(comparand, nameof(comparand), AssertResources.MessageComparandIsNull);

            int result = comparand.CompareTo(value);

            if (0 <= result)
            {
                throw new ArgumentOutOfRangeException(name, message);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is less than the specified comparand.
        /// </summary>
        /// <typeparam name="TValue">
        ///   A type that implements the <see cref="IComparable{T}"/> interface.
        /// </typeparam>
        /// <param name="value">
        ///   The parameter value to be tested.
        /// </param>
        /// <param name="comparand">
        ///   The value to compare against.
        /// </param>
        /// <param name="name">
        ///   The name of the parameter.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="comparand"/> is <see langword="null"/>.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///   Argument <paramref name="value"/> is less than <paramref name="comparand"/>.
        /// </exception>
        public static TValue ArgumentIsGreaterOrEqual<TValue>(TValue value, TValue comparand, string name, string message = null) where TValue : IComparable<TValue>
        {
            Assert.ArgumentIsNotNull(comparand, nameof(comparand), AssertResources.MessageComparandIsNull);

            int result = comparand.CompareTo(value);

            if (0 < result)
            {
                throw new ArgumentOutOfRangeException(name, message);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is greater than or equal to the specified
        ///   comparand.
        /// </summary>
        /// <typeparam name="TValue">
        ///   A type that implements the <see cref="IComparable{T}"/> interface.
        /// </typeparam>
        /// <param name="value">
        ///   The parameter value to be tested.
        /// </param>
        /// <param name="comparand">
        ///   The value to compare against.
        /// </param>
        /// <param name="name">
        ///   The name of the parameter.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="comparand"/> is <see langword="null"/>.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///   Argument <paramref name="value"/> is greater than or equal to <paramref name="comparand"/>.
        /// </exception>
        public static TValue ArgumentIsLess<TValue>(TValue value, TValue comparand, string name, string message = null) where TValue : IComparable<TValue>
        {
            Assert.ArgumentIsNotNull(comparand, nameof(comparand), AssertResources.MessageComparandIsNull);

            int result = comparand.CompareTo(value);

            if (0 >= result)
            {
                throw new ArgumentOutOfRangeException(name, message);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ArgumentException"/> if the specified value is greater than the specified comparand.
        /// </summary>
        /// <typeparam name="TValue">
        ///   A type that implements the <see cref="IComparable{T}"/> interface.
        /// </typeparam>
        /// <param name="value">
        ///   The parameter value to be tested.
        /// </param>
        /// <param name="comparand">
        ///   The value to compare against.
        /// </param>
        /// <param name="name">
        ///   The name of the parameter.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="comparand"/> is <see langword="null"/>.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///   Argument <paramref name="value"/> is less than <paramref name="comparand"/>.
        /// </exception>
        public static TValue ArgumentIsLessOrEqual<TValue>(TValue value, TValue comparand, string name, string message = null) where TValue : IComparable<TValue>
        {
            Assert.ArgumentIsNotNull(comparand, nameof(comparand), AssertResources.MessageComparandIsNull);

            int result = comparand.CompareTo(value);

            if (0 > result)
            {
                throw new ArgumentOutOfRangeException(name, message);
            }

            return value;
        }

        /// <summary>
        ///   Throws an <see cref="ObjectDisposedException"/> if the specified value indicating the disposed state of
        ///   an object is <see langword="true"/>.
        /// </summary>
        /// <param name="disposed">
        ///   A <see cref="bool"/> value indicating whether the object in question is disposed.
        /// </param>
        /// <param name="name">
        ///   The name of the object.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <exception cref="ObjectDisposedException">
        ///   Argument <paramref name="disposed"/> is <see langword="true"/>.
        /// </exception>
        public static void IsNotDisposed(bool disposed, string name, string message = null)
        {
            if (disposed == true)
            {
                Exception exception;

                if (null != message)
                {
                    exception = new ObjectDisposedException(name, message);
                }
                else
                {
                    exception = new ObjectDisposedException(name);
                }

                throw exception;
            }
        }

        /// <summary>
        ///   Throws an <see cref="InvalidOperationException"/> if the specified value indicating the current operation
        ///   is not valid.
        /// </summary>
        /// <param name="valid">
        ///   A <see cref="bool"/> value indicating whether the current operation is valid.
        /// </param>
        /// <param name="message">
        ///   The description of the error.
        /// </param>
        /// <exception cref="InvalidOperationException">
        ///   Argument <paramref name="valid"/> is <see langword="false"/>.
        /// </exception>
        public static void IsValidOperation(bool valid, string message = null)
        {
            if (valid == false)
            {
                Exception exception;

                if (message == null)
                {
                    exception = new InvalidOperationException();
                }
                else
                {
                    exception = new InvalidOperationException(message);
                }

                throw exception;
            }
        }
    }
}
