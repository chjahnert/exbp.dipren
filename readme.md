**DIPREN** stands for Distributed Processing Engine and is a framework that can be used to build tools that iterate through very large data sets and performs operations on each data item. Data processing tools built with **DIPREN** can be run on multiple machines to increase throughput. The size of the processing cluster can be changed dynamically while jobs are running.

**DIPREN** is implemented in a headless fashion which means that there is no central component responsible for orchestrating the work. Processing nodes communicate with each other through a database.

Among others, the following features are supported:
*	Scheduling and processing a distributed processing job
*	Monitoring the status of a distributed processing job
*	Dynamically increasing and shrinking the processing cluster

Each item in the data set has to have a unique key. The following key types are supported out-of-the-box:
*	UUID / GUID
*	32 bit signed integer
*	64 bit signed integer
*	Arbitrarily large signed integer
*	String

However, you can add support for your own unique key type with minimal effort by implementing an interface with a single method. By doing so, any data type can be supported including compound keys.
