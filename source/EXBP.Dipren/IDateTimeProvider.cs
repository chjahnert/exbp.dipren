
namespace EXBP.Dipren
{
    /// <summary>
    ///   The <see cref="IDateTimeProvider"/> interface allows a type to implement a component that returns the
    ///   current date and time.
    /// </summary>
    public interface IDateTimeProvider
    {
        /// <summary>
        ///   Returns the current date and time.
        /// </summary>
        /// <returns>
        ///   A <see cref="DateTime"/> value that contains the current date and time.
        /// </returns>
        DateTime GetDateTime();
    }
}
