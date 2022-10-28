
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements static methods for generating node identifiers.
    /// </summary>
    internal static class NodeIdentifier
    {
        internal const string DELIMITER = ":";

        private static long _instances = 0L;


        /// <summary>
        ///   Generates a new node identifier using a combination of machine name, process ID, application domain ID,
        ///   and an instance counter.
        /// </summary>
        /// <param name="type">
        ///   A <see cref="NodeType"/> value that indicates the type of the node for which the identifier is generated.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value that contains a new unique identifier for a node.
        /// </returns>
        internal static string Generate(NodeType type)
        {
            Assert.ArgumentIsDefined(type, nameof(type));

            long instanceId = Interlocked.Increment(ref NodeIdentifier._instances);

            string prefix;

            switch (type)
            {
                case NodeType.Scheduler:
                    prefix = "S";
                    break;

                case NodeType.Engine:
                    prefix = "E";
                    break;

                default:
                    throw new NotSupportedException();
            }

            string result = FormattableString.Invariant($"{prefix}{DELIMITER}{Environment.MachineName}{DELIMITER}{Environment.ProcessId}{DELIMITER}{AppDomain.CurrentDomain.Id}{DELIMITER}{instanceId}");

            return result;
        }
    }
}
