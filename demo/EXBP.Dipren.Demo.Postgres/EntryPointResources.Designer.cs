﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace EXBP.Dipren.Demo.Postgres {
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
    internal class EntryPointResources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal EntryPointResources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("EXBP.Dipren.Demo.Postgres.EntryPointResources", typeof(EntryPointResources).Assembly);
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
        ///   Looks up a localized string similar to Creates the required database structure and populates the source database..
        /// </summary>
        internal static string DescriptionCommandDeploy {
            get {
                return ResourceManager.GetString("DescriptionCommandDeploy", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Removes all database objects..
        /// </summary>
        internal static string DescriptionCommandRemove {
            get {
                return ResourceManager.GetString("DescriptionCommandRemove", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Dipren Demo Application using Postgres
        ///
        ///This application demonstrates the capabilities of the Dipren framework. It requires a Postgres SQL Server up and running..
        /// </summary>
        internal static string DescriptionCommandRoot {
            get {
                return ResourceManager.GetString("DescriptionCommandRoot", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Runs a distributed processing job..
        /// </summary>
        internal static string DescriptionCommandRun {
            get {
                return ResourceManager.GetString("DescriptionCommandRun", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Schedules a distributed processing processing job..
        /// </summary>
        internal static string DescriptionCommandSchedule {
            get {
                return ResourceManager.GetString("DescriptionCommandSchedule", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The connection string to the Postgres SQL database to use..
        /// </summary>
        internal static string DescriptionOptionDatabase {
            get {
                return ResourceManager.GetString("DescriptionOptionDatabase", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The number of rows to generate in the source database..
        /// </summary>
        internal static string DescriptionOptionDeployDatasetSize {
            get {
                return ResourceManager.GetString("DescriptionOptionDeployDatasetSize", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The name of the processing job..
        /// </summary>
        internal static string DescriptionOptionName {
            get {
                return ResourceManager.GetString("DescriptionOptionName", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The number of processing threads to start..
        /// </summary>
        internal static string DescriptionOptionRunThreads {
            get {
                return ResourceManager.GetString("DescriptionOptionRunThreads", resourceCulture);
            }
        }
    }
}
