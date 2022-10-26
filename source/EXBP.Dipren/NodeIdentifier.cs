
namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements static methods for generating node identifiers.
    /// </summary>
    internal static class NodeIdentifier
    {
        internal const string DELIMITER = "/";

        private static long _instances = 0L;

        /// <summary>
        ///   Generates a new node identifier using a combination of machine name, process ID, application domain ID,
        ///   and an instance counter.
        /// </summary>
        /// <returns>
        ///   A <see cref="string"/> value that contains a new unique identifier for a node.
        /// </returns>
        internal static string Generate()
        {
            long instanceId = Interlocked.Increment(ref NodeIdentifier._instances);

            string result = FormattableString.Invariant($"{Environment.MachineName}{DELIMITER}{Environment.ProcessId}{DELIMITER}{AppDomain.CurrentDomain.Id}{DELIMITER}{instanceId}");

            return result;
        }
    }
}
