
using System.Data.Common;


namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a <see cref="IErrorConditionClassifier"/> to be used with ADO.NET.
    /// </summary>
    public class DbErrorConditionClassifier : IErrorConditionClassifier
    {
        private readonly bool _default;


        /// <summary>
        ///   Initializes a new instance of the <see cref="DbErrorConditionClassifier"/> class.
        /// </summary>
        /// <param name="default">
        ///   The value to return if the exception specified is not a <see cref="DbException"/> instance.
        /// </param>
        public DbErrorConditionClassifier(bool @default = false)
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
