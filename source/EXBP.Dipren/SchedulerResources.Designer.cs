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
    internal class SchedulerResources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal SchedulerResources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("EXBP.Dipren.SchedulerResources", typeof(SchedulerResources).Assembly);
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
        ///   Looks up a localized string similar to Creating the initial partition..
        /// </summary>
        internal static string EventCreatingInitialPartition {
            get {
                return ResourceManager.GetString("EventCreatingInitialPartition", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Creating the distributed processing job..
        /// </summary>
        internal static string EventCreatingJob {
            get {
                return ResourceManager.GetString("EventCreatingJob", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Estimating the number of keys in the range..
        /// </summary>
        internal static string EventEstimatingRangeSize {
            get {
                return ResourceManager.GetString("EventEstimatingRangeSize", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The initial partition is persisted to the data store..
        /// </summary>
        internal static string EventInitialPartitionCreated {
            get {
                return ResourceManager.GetString("EventInitialPartitionCreated", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The distributed processing job is persisted to the data store..
        /// </summary>
        internal static string EventJobCreated {
            get {
                return ResourceManager.GetString("EventJobCreated", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The range boundaries are [{0}..{1}]. Took {2} ms. .
        /// </summary>
        internal static string EventRangeBoundariesRetrieved {
            get {
                return ResourceManager.GetString("EventRangeBoundariesRetrieved", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Range is estimated to contain {0} keys. Took {1} ms..
        /// </summary>
        internal static string EventRangeSizeEstimated {
            get {
                return ResourceManager.GetString("EventRangeSizeEstimated", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Retrieving range boundaries..
        /// </summary>
        internal static string EventRetrievingRangeBoundaries {
            get {
                return ResourceManager.GetString("EventRetrievingRangeBoundaries", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The specified timeout value might be too low..
        /// </summary>
        internal static string EventTimeoutValueTooLow {
            get {
                return ResourceManager.GetString("EventTimeoutValueTooLow", resourceCulture);
            }
        }
    }
}
