
namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a <see cref="ITransientErrorDetector"/> that always returns a predefined value.
    /// </summary>
    public sealed class PresetTransientErrorDetector : ITransientErrorDetector
    {
        private readonly bool _value;


        /// <summary>
        ///   Gets a <see cref="ITransientErrorDetector"/> instance that treats all exceptions as transient errors.
        /// </summary>
        /// <value>
        ///   A <see cref="ITransientErrorDetector"/> instance that treats all exceptions as transient errors.
        /// </value>
        public static ITransientErrorDetector AlwaysTransient { get; } = new PresetTransientErrorDetector(true);

        /// <summary>
        ///   Gets a <see cref="ITransientErrorDetector"/> instance that treats all exceptions as permanent errors.
        /// </summary>
        /// <value>
        ///   A <see cref="ITransientErrorDetector"/> instance that treats all exceptions as permanent errors.
        /// </value>
        public static ITransientErrorDetector NeverTransient  { get; } = new PresetTransientErrorDetector(false);


        /// <summary>
        ///   Initializes a new instance of the <see cref="PresetTransientErrorDetector"/> type.
        /// </summary>
        /// <param name="value">
        ///   <see langword="true"/> to treat all exceptions as transient errors; or <see langword="false"/> to treat
        ///   all exceptions as permanent errors.
        /// </param>
        private PresetTransientErrorDetector(bool value)
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
