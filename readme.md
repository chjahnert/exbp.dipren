**DIPREN** stands for Distributed Processing Engine and is a framework that allows you to iterate through very large data sets and perform operations on each data item. You can deploy your data processing tool on multiple processing nodes and process the entire data set. A new instance of the processing tool can be started at any time and it will automatically participate in the effort reducing the overall time it take to process the entire data set.

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

### Examples
Todo

* Database migration
* Image Conversion

### Getting started
Todo
