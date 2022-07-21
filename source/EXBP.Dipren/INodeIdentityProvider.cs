
namespace EXBP.Dipren
{
    /// <summary>
    ///   Allows a type to implement a node identifier provider.
    /// </summary>
    public interface INodeIdentityProvider
    {
        /// <summary>
        ///   Returns the unique identifier of the current processing node.
        /// </summary>
        /// <returns>
        ///   A <see cref="string"/> value that contains the unique identifier of the current processing node.
        /// </returns>
        string GetNodeIdentifier();
    }
}
