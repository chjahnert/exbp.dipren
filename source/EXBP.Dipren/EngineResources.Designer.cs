﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace EXBP.Dipren {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "17.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal class EngineResources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal EngineResources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("EXBP.Dipren.EngineResources", typeof(EngineResources).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to {0}..{1}.
        /// </summary>
        internal static string BatchDescriptior {
            get {
                return ResourceManager.GetString("BatchDescriptior", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Processed a batch of {0} items. Took {1} ms..
        /// </summary>
        internal static string EventBatchProcessed {
            get {
                return ResourceManager.GetString("EventBatchProcessed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to An error occurred during the processing of batch [{0}]..
        /// </summary>
        internal static string EventBatchProcessingFailed {
            get {
                return ResourceManager.GetString("EventBatchProcessingFailed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Retrieved a batch of {0} items. Took {1} ms..
        /// </summary>
        internal static string EventBatchRetrieved {
            get {
                return ResourceManager.GetString("EventBatchRetrieved", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The partition could not be split..
        /// </summary>
        internal static string EventCouldNotSplitPartition {
            get {
                return ResourceManager.GetString("EventCouldNotSplitPartition", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Job completed..
        /// </summary>
        internal static string EventJobCompleted {
            get {
                return ResourceManager.GetString("EventJobCompleted", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The specified job is not scheduled..
        /// </summary>
        internal static string EventJobNotScheduled {
            get {
                return ResourceManager.GetString("EventJobNotScheduled", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Job started..
        /// </summary>
        internal static string EventJobStarted {
            get {
                return ResourceManager.GetString("EventJobStarted", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Partition acquired..
        /// </summary>
        internal static string EventPartitionAcquired {
            get {
                return ResourceManager.GetString("EventPartitionAcquired", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Partition completed..
        /// </summary>
        internal static string EventPartitionCompleted {
            get {
                return ResourceManager.GetString("EventPartitionCompleted", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Could not acquire partition. None free or abandoned..
        /// </summary>
        internal static string EventPartitionNotAcquired {
            get {
                return ResourceManager.GetString("EventPartitionNotAcquired", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Partition updated to [{0}..{1}]. New partition {2} created with range [{3}..{4}]. Took {5} ms.
        /// </summary>
        internal static string EventPartitionSplit {
            get {
                return ResourceManager.GetString("EventPartitionSplit", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The partition was taken by another processing node..
        /// </summary>
        internal static string EventPartitionTaken {
            get {
                return ResourceManager.GetString("EventPartitionTaken", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The partition is too small to be split..
        /// </summary>
        internal static string EventPartitionTooSmallToBeSplit {
            get {
                return ResourceManager.GetString("EventPartitionTooSmallToBeSplit", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Processing failed..
        /// </summary>
        internal static string EventProcessingFailed {
            get {
                return ResourceManager.GetString("EventProcessingFailed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Processing partition..
        /// </summary>
        internal static string EventProcessingPartition {
            get {
                return ResourceManager.GetString("EventProcessingPartition", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Requesting {0} items from range [{1}..{2}]. Skipping {3}..
        /// </summary>
        internal static string EventRequestingNextBatch {
            get {
                return ResourceManager.GetString("EventRequestingNextBatch", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Requesting a partition to be split..
        /// </summary>
        internal static string EventRequestingSplit {
            get {
                return ResourceManager.GetString("EventRequestingSplit", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Partition split requested..
        /// </summary>
        internal static string EventSplitRequested {
            get {
                return ResourceManager.GetString("EventSplitRequested", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to No suitable partitions available..
        /// </summary>
        internal static string EventSplitRequestFailed {
            get {
                return ResourceManager.GetString("EventSplitRequestFailed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Successfully requested a split..
        /// </summary>
        internal static string EventSplitRequestSucceeded {
            get {
                return ResourceManager.GetString("EventSplitRequestSucceeded", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Processing the batch took longer than the specified timeout. The timeout value specified might be too low..
        /// </summary>
        internal static string EventTimeoutValueTooLow {
            get {
                return ResourceManager.GetString("EventTimeoutValueTooLow", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Trying to acquire a partition..
        /// </summary>
        internal static string EventTryingToAcquirePartition {
            get {
                return ResourceManager.GetString("EventTryingToAcquirePartition", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Waiting for the job to be ready..
        /// </summary>
        internal static string EventWaitingForJobToBeReady {
            get {
                return ResourceManager.GetString("EventWaitingForJobToBeReady", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to A distributed processing job with the specified identifier is not scheduled..
        /// </summary>
        internal static string NoJobScheduledWithSpecifiedIdentifier {
            get {
                return ResourceManager.GetString("NoJobScheduledWithSpecifiedIdentifier", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The key arithmetics implementation returned too many key ranges..
        /// </summary>
        internal static string RangeSplitIntoTooManyRanges {
            get {
                return ResourceManager.GetString("RangeSplitIntoTooManyRanges", resourceCulture);
            }
        }
    }
}
