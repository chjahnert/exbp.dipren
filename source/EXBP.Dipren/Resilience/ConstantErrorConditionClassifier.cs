
namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a <see cref="IErrorConditionClassifier"/> that always returns a predefined value.
    /// </summary>
    internal sealed class ConstantErrorConditionClassifier : IErrorConditionClassifier
    {
        /// <summary>
        ///   Gets a <see cref="IErrorConditionClassifier"/> instance that treats all exceptions as transient errors.
        /// </summary>
        /// <value>
        ///   A <see cref="IErrorConditionClassifier"/> instance that treats all exceptions as transient errors.
        /// </value>
        public static IErrorConditionClassifier AlwaysTransient { get; } = new ConstantErrorConditionClassifier(true);

        /// <summary>
        ///   Gets a <see cref="IErrorConditionClassifier"/> instance that treats all exceptions as permanent errors.
        /// </summary>
        /// <value>
        ///   A <see cref="IErrorConditionClassifier"/> instance that treats all exceptions as permanent errors.
        /// </value>
        public static IErrorConditionClassifier NeverTransient  { get; } = new ConstantErrorConditionClassifier(false);


        private readonly bool _value;


        /// <summary>
        ///   Initializes a new instance of the <see cref="ConstantErrorConditionClassifier"/> type.
        /// </summary>
        /// <param name="value">
        ///   <see langword="true"/> to treat all exceptions as transient errors; or <see langword="false"/> to treat
        ///   all exceptions as permanent errors.
        /// </param>
        private ConstantErrorConditionClassifier(bool value)
        {
            this._value = value;
        }

        /// <summary>
        ///   Determines whether the specified exception represents a transient error condition.
        /// </summary>
        /// <param name="exception">
        ///   The exception in question.
        /// </param>
        /// <returns>
        ///   <see langword="true"/> if <paramref name="exception"/> represents a transient error condition; otherwise
        ///   <see langword="false"/>.
        /// </returns>
        public bool IsTransientError(Exception exception)
            => this._value;
    }
}
