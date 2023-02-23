
namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Enumerates engine states.
    /// </summary>
    public enum EngineState
    {
        /// <summary>
        ///   The engine is ready to start processing. It is associated with a distributed processing job and is
        ///   trying to acquire a partition.
        /// </summary>
        Ready,

        /// <summary>
        ///   The engine is processing a partition.
        /// </summary>
        Processing,

        /// <summary>
        ///   The processing job is completed or the engine existed due to an error it could not recover from.
        /// </summary>
        Stopped
    }
}
