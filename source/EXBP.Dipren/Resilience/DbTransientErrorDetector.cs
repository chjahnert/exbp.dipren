
using System.Data.Common;


namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements an <see cref="ITransientErrorDetector"/> that uses the <see cref="DbException.IsTransient"/>
    ///   property to detect transient error conditions.
    /// </summary>
    public class DbTransientErrorDetector : ITransientErrorDetector
    {
        private readonly bool _default;


        /// <summary>
        ///   Gets the default instance of the <see cref="DbTransientErrorDetector"/> class.
        /// </summary>
        /// <value>
        ///   A <see cref="DbTransientErrorDetector"/> instance that is ready to be used.
        /// </value>
        public static DbTransientErrorDetector Default { get; } = new DbTransientErrorDetector(false);


        /// <summary>
        ///   Initializes a new instance of the <see cref="DbTransientErrorDetector"/> class.
        /// </summary>
        /// <param name="default">
        ///   <see langword="true"/> to threat all exceptions that are not derived from <see cref="DbException"/> as
        ///   transient errors; otherwise <see langword="false"/>. The default value is <see langword="false"/>.
        /// </param>
        public DbTransientErrorDetector(bool @default = false)
        {
            this._default = @default;
        }

        /// <summary>
        ///   Determines whether the specified exception represents a transient error condition.
        /// </summary>
        /// <param name="exception">
        ///   The exception in question.
        /// </param>
        /// <returns>
        ///   <see langword="true"/> if <paramref name="exception"/> is of type <see cref="DbException"/> and the
        ///   <see cref="DbException.IsTransient"/> property is returning <see langword="true"/>; otherwise the default
        ///   value specified at construction time.
        /// </returns>
        public bool IsTransientError(Exception exception)
            => ((exception as DbException)?.IsTransient ?? this._default);
    }
}
