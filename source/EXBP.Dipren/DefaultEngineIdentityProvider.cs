
namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements an <see cref="IEngineIdentityProvider"/> that uses a combination of machine name, process ID,
    ///   application domain ID, and an instance counter to build the unique engine identifier.
    /// </summary>
    internal sealed class DefaultEngineIdentityProvider : IEngineIdentityProvider
    {
        internal const string DELIMITER = "/";

        private static long _instances = 0L;

        private readonly string _value;


        /// <summary>
        ///   Initializes a new instance of the <see cref="DefaultEngineIdentityProvider"/> class.
        /// </summary>
        internal DefaultEngineIdentityProvider()
        {
            long instanceId = Interlocked.Increment(ref DefaultEngineIdentityProvider._instances);

            this._value = FormattableString.Invariant($"{Environment.MachineName}{DELIMITER}{Environment.ProcessId}{DELIMITER}{AppDomain.CurrentDomain.Id}{DELIMITER}{instanceId}");
        }

        /// <summary>
        ///   Returns the unique identifier of the current processing engine.
        /// </summary>
        /// <returns>
        ///   A <see cref="string"/> value that contains the unique identifier of the current processing node.
        /// </returns>
        /// <remarks>
        ///   The returned value is structured like <c>machine-name/process id/app domain id/instance id</c>.
        /// </remarks>
        public string GetEngineIdentifier()
            => this._value;
    }
}
