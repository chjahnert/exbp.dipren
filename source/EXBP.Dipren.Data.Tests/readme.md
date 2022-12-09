Implements an NUnit test set for verifying DIPREN Data Store implementations.

To implement the test set, create a new test fixture that derives from the `EXBP.Dipren.Data.Tests.EngineDataStoreTests`
type and implement the abstract method that instantiates the data store to be tested. To deploy the database schema
required by the tests and clean up after the tests completed, implement methods decorated with the `[SetUp]` and
`[TearDown]` attributes as you would do normally.
