﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace EXBP.Dipren.Data.SQLite {
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
    internal class SQLiteEngineDataStoreResources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal SQLiteEngineDataStoreResources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("EXBP.Dipren.Data.SQLite.SQLiteEngineDataStoreResources", typeof(SQLiteEngineDataStoreResources).Assembly);
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
        ///   Looks up a localized string similar to CREATE TABLE IF NOT EXISTS &quot;jobs&quot;
        ///(
        ///  &quot;id&quot; VARCHAR(256) NOT NULL,
        ///  &quot;created&quot; DATETIME NOT NULL,
        ///  &quot;updated&quot; DATETIME NOT NULL,
        ///  &quot;state&quot; INTEGER NOT NULL,
        ///  &quot;exception&quot; TEXT NULL,
        ///  
        ///  CONSTRAINT &quot;pk_jobs&quot; PRIMARY KEY (&quot;id&quot;)
        ///)
        ///WITHOUT ROWID;
        ///
        ///
        ///CREATE TABLE IF NOT EXISTS &quot;partitions&quot;
        ///(
        ///  &quot;id&quot; CHAR(36) NOT NULL,
        ///  &quot;job_id&quot; VARCHAR(256) NOT NULL,
        ///  &quot;created&quot; DATETIME NOT NULL,
        ///  &quot;updated&quot; DATETIME NOT NULL,
        ///  &quot;owner&quot; VARCHAR(256) NULL,
        ///  &quot;first&quot; TEXT NOT NULL,
        ///  &quot;last&quot; TEXT NOT NULL,
        ///  &quot; [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string SqlCreateSchema {
            get {
                return ResourceManager.GetString("SqlCreateSchema", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to INSERT INTO &quot;jobs&quot;
        ///(
        ///  &quot;id&quot;,
        ///  &quot;created&quot;,
        ///  &quot;updated&quot;,
        ///  &quot;state&quot;,
        ///  &quot;exception&quot;
        ///)
        ///VALUES
        ///(
        ///  $id,
        ///  $created,
        ///  $updated,
        ///  $state,
        ///  $exception
        ///);.
        /// </summary>
        internal static string SqlInsertJob {
            get {
                return ResourceManager.GetString("SqlInsertJob", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to INSERT INTO &quot;partitions&quot;
        ///(
        ///  &quot;id&quot;,
        ///  &quot;job_id&quot;,
        ///  &quot;created&quot;,
        ///  &quot;updated&quot;,
        ///  &quot;owner&quot;,
        ///  &quot;first&quot;,
        ///  &quot;last&quot;,
        ///  &quot;inclusive&quot;,
        ///  &quot;position&quot;,
        ///  &quot;processed&quot;,
        ///  &quot;remaining&quot;,
        ///  &quot;is_completed&quot;,
        ///  &quot;is_split_requested&quot;
        ///)
        ///VALUES
        ///(
        ///  $id,
        ///  $job_id,
        ///  $created,
        ///  $updated,
        ///  $owner,
        ///  $first,
        ///  $last,
        ///  $inclusive,
        ///  $position,
        ///  $processed,
        ///  $remaining,
        ///  $is_completed,
        ///  $is_split_requested
        ///);.
        /// </summary>
        internal static string SqlInsertPartition {
            get {
                return ResourceManager.GetString("SqlInsertPartition", resourceCulture);
            }
        }
    }
}