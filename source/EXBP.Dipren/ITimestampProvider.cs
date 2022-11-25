
namespace EXBP.Dipren
{
    /// <summary>
    ///   The <see cref="ITimestampProvider"/> interface allows a type to implement a component that returns the
    ///   current timestamp as a <see cref="DateTime"/> value.
    /// </summary>
    public interface ITimestampProvider
    {
        /// <summary>
        ///   Returns the current timestamp.
        /// </summary>
        /// <returns>
        ///   A <see cref="DateTime"/> value that contains the current timestamp.
        /// </returns>
        DateTime GetCurrentTimestamp();
    }
}
