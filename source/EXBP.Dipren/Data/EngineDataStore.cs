
namespace EXBP.Dipren.Data
{
    /// <summary>
    ///    Serves as a base class for engine data store implementations.
    /// </summary>
    public abstract class EngineDataStore
    {
        /// <summary>
        ///   Initializes a new instance of the <see cref="EngineDataStore"/> class.
        /// </summary>
        protected EngineDataStore()
        {
        }

        /// <summary>
        ///   Throws a <see cref="UnknownIdentifierException"/> exception indicating that the specified job identifier
        ///   could not be found in the data store.
        /// </summary>
        /// <param name="inner">
        ///   The exception that cause the current error condition.
        /// </param>
        /// <exception cref="UnknownIdentifierException">
        ///   The specified job identifier could not be found in the data store.
        /// </exception>
        protected virtual void RaiseErrorUnknownJobIdentifier(Exception inner = null)
            => throw new UnknownIdentifierException(EngineDataStoreResources.JobWithSpecifiedIdentifierDoesNotExist, inner);

        /// <summary>
        ///   Throws a <see cref="UnknownIdentifierException"/> exception indicating that the specified partition
        ///   identifier could not be found in the data store.
        /// </summary>
        /// <param name="inner">
        ///   The exception that cause the current error condition.
        /// </param>
        /// <exception cref="UnknownIdentifierException">
        ///   The specified partition identifier could not be found in the data store.
        /// </exception>
        protected virtual void RaiseErrorUnknownPartitionIdentifier(Exception inner = null)
            => throw new UnknownIdentifierException(EngineDataStoreResources.PartitionWithSpecifiedIdentifierDoesNotExist, inner);

        /// <summary>
        ///   Throws a <see cref="DuplicateIdentifierException"/> exception indicating that the specified job
        ///   identifier is already in use.
        /// </summary>
        /// <param name="inner">
        ///   The exception that cause the current error condition.
        /// </param>
        /// <exception cref="DuplicateIdentifierException">
        ///   The specified job identifier is already in use.
        /// </exception>
        protected virtual void RaiseErrorDuplicateJobIdentifier(Exception inner = null)
            => throw new DuplicateIdentifierException(EngineDataStoreResources.JobWithSameIdentiferAlreadyExists, inner);

        /// <summary>
        ///   Throws a <see cref="DuplicateIdentifierException"/> exception indicating that the specified partition
        ///   identifier is already in use.
        /// </summary>
        /// <param name="inner">
        ///   The exception that cause the current error condition.
        /// </param>
        /// <exception cref="DuplicateIdentifierException">
        ///   The specified partition identifier is already in use.
        /// </exception>
        protected virtual void RaiseErrorDuplicatePartitionIdentifier(Exception inner = null)
            => throw new DuplicateIdentifierException(EngineDataStoreResources.PartitionWithSameIdentifierAlreadyExists, inner);

        /// <summary>
        ///   Throws an <see cref="InvalidReferenceException"/> exception indicating that the specified reference is
        ///   not valid or the referenced object does not exist.
        /// </summary>
        /// <param name="inner">
        ///   The exception that cause the current error condition.
        /// </param>
        /// <exception cref="InvalidReferenceException">
        ///   The specified reference is not valid or the referenced object does not exist.
        /// </exception>
        protected virtual void RaiseErrorInvalidJobReference(Exception inner = null)
            => throw new InvalidReferenceException(EngineDataStoreResources.ReferencedJobDoesNotExist, inner);

        /// <summary>
        ///   Throws an <see cref="LockException"/> exception indicating that the lock on the object is no longer held.
        /// </summary>
        /// <param name="inner">
        ///   The exception that cause the current error condition.
        /// </param>
        /// <exception cref="LockException">
        ///   The lock on the object is no longer held.
        /// </exception>
        protected virtual void RaiseErrorLockNoLongerHeld(Exception inner = null)
            => throw new LockException(EngineDataStoreResources.PartitionLockNoLongerHeld, inner);
    }
}
