Implements an [NUnit](https://nunit.org/) test set for verifying DIPREN Engine Data Store implementations.

To implement the tests, create a new test fixture that derives from the `EXBP.Dipren.Data.Tests.EngineDataStoreTests` type and implement the abstract method that instantiates the data store to be tested. If the Engine Data Store implementation requires a database schema, add methods decorated with the `[SetUp]` and `[TearDown]` attributes that perform the required deployment and clean up tasks.
