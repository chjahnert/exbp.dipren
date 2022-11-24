
namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Allows a type to implement an error condition classifier that determines whether an error condition is
    ///   transient or permanent.
    /// </summary>
    public interface IErrorConditionClassifier
    {
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
        bool IsTransientError(Exception exception);
    }
}
