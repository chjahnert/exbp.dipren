﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace EXBP.Dipren.Data.Postgres {
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
    internal class PostgresEngineDataStoreResources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal PostgresEngineDataStoreResources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("EXBP.Dipren.Data.Postgres.PostgresEngineDataStoreResources", typeof(PostgresEngineDataStoreResources).Assembly);
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
        ///   Looks up a localized string similar to SELECT
        ///  (SELECT COUNT(1) FROM &quot;dipren&quot;.&quot;jobs&quot; WHERE (&quot;id&quot; = @job_id)) AS &quot;job_count&quot;,
        ///  (SELECT COUNT(1) FROM &quot;dipren&quot;.&quot;partitions&quot; WHERE (&quot;job_id&quot; = @job_id) AND (&quot;is_completed&quot; = FALSE)) AS &quot;partition_count&quot;;.
        /// </summary>
        internal static string QueryCountIncompletePartitions {
            get {
                return ResourceManager.GetString("QueryCountIncompletePartitions", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to SELECT
        ///  COUNT(1) AS &quot;count&quot;
        ///FROM
        ///  &quot;dipren&quot;.&quot;jobs&quot;;.
        /// </summary>
        internal static string QueryCountJobs {
            get {
                return ResourceManager.GetString("QueryCountJobs", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to SELECT
        ///  COUNT(1) AS &quot;count&quot;
        ///FROM
        ///  &quot;dipren&quot;.&quot;jobs&quot;
        ///WHERE
        ///  (&quot;id&quot; = @id);.
        /// </summary>
        internal static string QueryDoesJobExist {
            get {
                return ResourceManager.GetString("QueryDoesJobExist", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to SELECT
        ///  COUNT(1) AS &quot;count&quot;
        ///FROM
        ///  &quot;dipren&quot;.&quot;partitions&quot;
        ///WHERE
        ///  (&quot;id&quot; = @id);.
        /// </summary>
        internal static string QueryDoesPartitionExist {
            get {
                return ResourceManager.GetString("QueryDoesPartitionExist", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to INSERT INTO &quot;dipren&quot;.&quot;jobs&quot;
        ///(
        ///  &quot;id&quot;,
        ///  &quot;created&quot;,
        ///  &quot;updated&quot;,
        ///  &quot;started&quot;,
        ///  &quot;completed&quot;,
        ///  &quot;state&quot;,
        ///  &quot;error&quot;
        ///)
        ///VALUES
        ///(
        ///  @id,
        ///  @created,
        ///  @updated,
        ///  @started,
        ///  @completed,
        ///  @state,
        ///  @error
        ///);.
        /// </summary>
        internal static string QueryInsertJob {
            get {
                return ResourceManager.GetString("QueryInsertJob", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to INSERT INTO &quot;dipren&quot;.&quot;partitions&quot;
        ///(
        ///  &quot;id&quot;,
        ///  &quot;job_id&quot;,
        ///  &quot;created&quot;,
        ///  &quot;updated&quot;,
        ///  &quot;owner&quot;,
        ///  &quot;first&quot;,
        ///  &quot;last&quot;,
        ///  &quot;is_inclusive&quot;,
        ///  &quot;position&quot;,
        ///  &quot;processed&quot;,
        ///  &quot;remaining&quot;,
        ///  &quot;is_completed&quot;,
        ///  &quot;is_split_requested&quot;
        ///)
        ///VALUES
        ///(
        ///  @id,
        ///  @job_id,
        ///  @created,
        ///  @updated,
        ///  @owner,
        ///  @first,
        ///  @last,
        ///  @is_inclusive,
        ///  @position,
        ///  @processed,
        ///  @remaining,
        ///  @is_completed,
        ///  @is_split_requested
        ///);.
        /// </summary>
        internal static string QueryInsertPartition {
            get {
                return ResourceManager.GetString("QueryInsertPartition", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to UPDATE
        ///  &quot;dipren&quot;.&quot;jobs&quot;
        ///SET
        ///  &quot;updated&quot; = @timestamp,
        ///  &quot;completed&quot; = @timestamp,
        ///  &quot;state&quot; = @state
        ///WHERE
        ///  (&quot;id&quot; = @id)
        ///RETURNING
        ///  &quot;id&quot; AS &quot;id&quot;,
        ///  &quot;created&quot; AS &quot;created&quot;,
        ///  &quot;updated&quot; AS &quot;updated&quot;,
        ///  &quot;started&quot; AS &quot;started&quot;,
        ///  &quot;completed&quot; AS &quot;completed&quot;,
        ///  &quot;state&quot; AS &quot;state&quot;,
        ///  &quot;error&quot; AS &quot;error&quot;;.
        /// </summary>
        internal static string QueryMarkJobCompleted {
            get {
                return ResourceManager.GetString("QueryMarkJobCompleted", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to UPDATE
        ///  &quot;dipren&quot;.&quot;jobs&quot;
        ///SET
        ///  &quot;updated&quot; = @timestamp,
        ///  &quot;state&quot; = @state,
        ///  &quot;error&quot; = @error
        ///WHERE
        ///  (&quot;id&quot; = @id)
        ///RETURNING
        ///  &quot;id&quot; AS &quot;id&quot;,
        ///  &quot;created&quot; AS &quot;created&quot;,
        ///  &quot;updated&quot; AS &quot;updated&quot;,
        ///  &quot;started&quot; AS &quot;started&quot;,
        ///  &quot;completed&quot; AS &quot;completed&quot;,
        ///  &quot;state&quot; AS &quot;state&quot;,
        ///  &quot;error&quot; AS &quot;error&quot;;.
        /// </summary>
        internal static string QueryMarkJobFailed {
            get {
                return ResourceManager.GetString("QueryMarkJobFailed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to UPDATE
        ///  &quot;dipren&quot;.&quot;jobs&quot;
        ///SET
        ///  &quot;updated&quot; = @timestamp,
        ///  &quot;started&quot; = @timestamp,
        ///  &quot;state&quot; = @state
        ///WHERE
        ///  (&quot;id&quot; = @id)
        ///RETURNING
        ///  &quot;id&quot; AS &quot;id&quot;,
        ///  &quot;created&quot; AS &quot;created&quot;,
        ///  &quot;updated&quot; AS &quot;updated&quot;,
        ///  &quot;started&quot; AS &quot;started&quot;,
        ///  &quot;completed&quot; AS &quot;completed&quot;,
        ///  &quot;state&quot; AS &quot;state&quot;,
        ///  &quot;error&quot; AS &quot;error&quot;;.
        /// </summary>
        internal static string QueryMarkJobStarted {
            get {
                return ResourceManager.GetString("QueryMarkJobStarted", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to UPDATE
        ///  &quot;dipren&quot;.&quot;partitions&quot;
        ///SET
        ///  &quot;updated&quot; = @updated,
        ///  &quot;position&quot; = @position,
        ///  &quot;processed&quot; = (&quot;processed&quot; + @progress),
        ///  &quot;remaining&quot; = (&quot;remaining&quot; - @progress),
        ///  &quot;is_completed&quot; = @completed
        ///WHERE
        ///  (&quot;id&quot; = @id) AND
        ///  (&quot;owner&quot; = @owner)
        ///RETURNING
        ///  &quot;id&quot; AS &quot;id&quot;,
        ///  &quot;job_id&quot; AS &quot;job_id&quot;,
        ///  &quot;created&quot; AS &quot;created&quot;,
        ///  &quot;updated&quot; AS &quot;updated&quot;,
        ///  &quot;owner&quot; AS &quot;owner&quot;,
        ///  &quot;first&quot; AS &quot;first&quot;,
        ///  &quot;last&quot; AS &quot;last&quot;,
        ///  &quot;is_inclusive&quot; AS &quot;is_inclusive&quot;,
        ///  &quot;position&quot; AS &quot;position&quot;,
        ///  &quot;processed&quot;  [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string QueryReportProgress {
            get {
                return ResourceManager.GetString("QueryReportProgress", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to SELECT
        ///  &quot;id&quot; AS &quot;id&quot;,
        ///  &quot;created&quot; AS &quot;created&quot;,
        ///  &quot;updated&quot; AS &quot;updated&quot;,
        ///  &quot;started&quot; AS &quot;started&quot;,
        ///  &quot;completed&quot; AS &quot;completed&quot;,
        ///  &quot;state&quot; AS &quot;state&quot;,
        ///  &quot;error&quot; AS &quot;error&quot;
        ///FROM
        ///  &quot;dipren&quot;.&quot;jobs&quot;
        ///WHERE
        ///  (&quot;id&quot; = @id);.
        /// </summary>
        internal static string QueryRetrieveJobById {
            get {
                return ResourceManager.GetString("QueryRetrieveJobById", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to SELECT
        ///  &quot;id&quot; AS &quot;id&quot;,
        ///  &quot;job_id&quot; AS &quot;job_id&quot;,
        ///  &quot;created&quot; AS &quot;created&quot;,
        ///  &quot;updated&quot; AS &quot;updated&quot;,
        ///  &quot;owner&quot; AS &quot;owner&quot;,
        ///  &quot;first&quot; AS &quot;first&quot;,
        ///  &quot;last&quot; AS &quot;last&quot;,
        ///  &quot;is_inclusive&quot; AS &quot;is_inclusive&quot;,
        ///  &quot;position&quot; AS &quot;position&quot;,
        ///  &quot;processed&quot; AS &quot;processed&quot;,
        ///  &quot;remaining&quot; AS &quot;remaining&quot;,
        ///  &quot;is_completed&quot; AS &quot;is_completed&quot;,
        ///  &quot;is_split_requested&quot; AS &quot;is_split_requested&quot;
        ///FROM
        ///  &quot;dipren&quot;.&quot;partitions&quot;
        ///WHERE
        ///  (&quot;id&quot; = @id);.
        /// </summary>
        internal static string QueryRetrievePartitionById {
            get {
                return ResourceManager.GetString("QueryRetrievePartitionById", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to WITH &quot;candidate&quot; AS
        ///(
        ///  SELECT
        ///    &quot;id&quot;
        ///  FROM
        ///    &quot;dipren&quot;.&quot;partitions&quot;
        ///  WHERE
        ///    (&quot;job_id&quot; = @job_id) AND
        ///    ((&quot;owner&quot; IS NULL) OR (&quot;updated&quot; &lt; @active)) AND
        ///    (&quot;is_completed&quot; = FALSE)
        ///  ORDER BY
        ///    &quot;remaining&quot; DESC
        ///  LIMIT
        ///    1
        ///  FOR UPDATE
        ///)
        ///UPDATE
        ///  &quot;dipren&quot;.&quot;partitions&quot; AS &quot;target&quot;
        ///SET
        ///  &quot;updated&quot; = @updated,
        ///  &quot;owner&quot; = @owner,
        ///  &quot;acquired&quot; = (&quot;acquired&quot; + 1)
        ///FROM
        ///  &quot;candidate&quot;
        ///WHERE
        ///  (&quot;target&quot;.&quot;id&quot; = &quot;candidate&quot;.&quot;id&quot;)
        ///RETURNING
        ///  &quot;target&quot;.&quot;id&quot; AS &quot;id&quot;,
        ///  &quot;job_id&quot;  [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string QueryTryAcquirePartition {
            get {
                return ResourceManager.GetString("QueryTryAcquirePartition", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to WITH &quot;candidate&quot; AS
        ///(
        ///  SELECT
        ///    &quot;id&quot;
        ///  FROM
        ///    &quot;dipren&quot;.&quot;partitions&quot;
        ///  WHERE
        ///    (&quot;job_id&quot; = @job_id) AND
        ///    (&quot;owner&quot; IS NOT NULL) AND
        ///    (&quot;updated&quot; &gt;= @active) AND
        ///    (&quot;is_completed&quot; = FALSE) AND
        ///    (&quot;is_split_requested&quot; = FALSE)
        ///  ORDER BY
        ///    &quot;remaining&quot; DESC
        ///  LIMIT
        ///    1
        ///  FOR UPDATE
        ///)
        ///UPDATE
        ///  &quot;dipren&quot;.&quot;partitions&quot; AS &quot;target&quot;
        ///SET
        ///  &quot;is_split_requested&quot; = TRUE
        ///FROM
        ///  &quot;candidate&quot;
        ///WHERE
        ///  (&quot;target&quot;.&quot;id&quot; = &quot;candidate&quot;.&quot;id&quot;);.
        /// </summary>
        internal static string QueryTryRequestSplit {
            get {
                return ResourceManager.GetString("QueryTryRequestSplit", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to UPDATE
        ///  &quot;dipren&quot;.&quot;jobs&quot;
        ///SET
        ///  &quot;updated&quot; = @updated,
        ///  &quot;state&quot; = @state,
        ///  &quot;error&quot; = @error
        ///WHERE
        ///  (&quot;id&quot; = @id)
        ///RETURNING
        ///  &quot;id&quot; AS &quot;id&quot;,
        ///  &quot;created&quot; AS &quot;created&quot;,
        ///  &quot;updated&quot; AS &quot;updated&quot;,
        ///  &quot;started&quot; AS &quot;started&quot;,
        ///  &quot;completed&quot; AS &quot;completed&quot;,
        ///  &quot;state&quot; AS &quot;state&quot;,
        ///  &quot;error&quot; AS &quot;error&quot;;.
        /// </summary>
        internal static string QueryUpdateJobById {
            get {
                return ResourceManager.GetString("QueryUpdateJobById", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to UPDATE
        ///  &quot;dipren&quot;.&quot;partitions&quot;
        ///SET
        ///  &quot;updated&quot; = @updated,
        ///  &quot;last&quot; = @last,
        ///  &quot;is_inclusive&quot; = @is_inclusive,
        ///  &quot;position&quot; = @position,
        ///  &quot;processed&quot; = @processed,
        ///  &quot;remaining&quot; = @remaining,
        ///  &quot;is_split_requested&quot; = @is_split_requested
        ///WHERE
        ///  (&quot;id&quot; = @partition_id) AND
        ///  (&quot;owner&quot; = @owner);.
        /// </summary>
        internal static string QueryUpdateSplitPartition {
            get {
                return ResourceManager.GetString("QueryUpdateSplitPartition", resourceCulture);
            }
        }
    }
}
