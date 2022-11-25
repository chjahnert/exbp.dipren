
namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Allows a type to implement a method for determining whether an error condition is a transient error and maybe
    ///   retried without modifications.
    /// </summary>
    public interface ITransientErrorDetector
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
