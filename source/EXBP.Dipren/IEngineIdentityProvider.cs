
namespace EXBP.Dipren
{
    /// <summary>
    ///   Allows a type to implement an engine identity provider.
    /// </summary>
    public interface IEngineIdentityProvider
    {
        /// <summary>
        ///   Returns the unique identifier of the current processing engine.
        /// </summary>
        /// <returns>
        ///   A <see cref="string"/> value that contains the unique identifier of the current processing engine.
        /// </returns>
        string GetEngineIdentifier();
    }
}
