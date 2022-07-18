
using System.Diagnostics;

namespace EXBP.Dipren
{
    /// <summary>
    ///   Holds information about a processing node.
    /// </summary>
    [DebuggerDisplay("Machine Name = {MachineName}, Process Path = {ProcessName}, Process ID = {ProcessId}")]
    public class NodeInfo
    {
        public static readonly NodeInfo Current = new NodeInfo(Environment.MachineName, Environment.ProcessPath, Environment.ProcessId);


        private readonly string _machineName;
        private readonly string _processName;
        private readonly int _processId;


        /// <summary>
        ///   Gets the name of the current machine.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the name of the current machine.
        /// </value>
        public string MachineName => _machineName;

        /// <summary>
        ///   Gets the path of the executable file that started the current executing process.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the path of the executable file that started the current
        ///   executing process; or <see langword="null"/> if the process path is not available.
        /// </value>
        public string ProcessName => _processName;

        /// <summary>
        ///   Gets the unique identifier of the current process.
        /// </summary>
        /// <value>
        ///   A <see cref="int"/> value that contains the unique identifier of the current process.
        /// </value>
        public int ProcessId => _processId;


        /// <summary>
        ///   Initializes a new instance of the <see cref="NodeInfo"/> class.
        /// </summary>
        /// <param name="machineName">
        ///   The name of the current machine.
        /// </param>
        /// <param name="processName">
        ///   The path of the executable file that started the current executing process.
        /// </param>
        /// <param name="processId">
        ///   The unique identifier of the current process.
        /// </param>
        public NodeInfo(string machineName, string processName, int processId)
        {
            this._machineName = machineName;
            this._processName = processName;
            this._processId = processId;
        }
    }
}
