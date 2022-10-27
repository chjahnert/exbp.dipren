﻿
using EXBP.Dipren.Telemetry;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Telemetry
{
    [TestFixture]
    public class CompositeEventHandlerTests
    {
        [Test]
        public void Ctor_ArgumentHandlersIsNull_ThrowsException()
        {
            IEnumerable<IEventHandler> handlers = null;

            Assert.Throws<ArgumentNullException>(() => new CompositeEventHandler(handlers));
        }

        [Test]
        public void Ctor_ArgumentHandlersIsEmpty_CreatesInstance()
        {
            IEnumerable<IEventHandler> handlers = Enumerable.Empty<IEventHandler>();
            IEventHandler composite = new CompositeEventHandler(handlers);
        }

        [Test]
        public void HandleEventAsync_ArgumentDescriptorIsNull_ThrowsException()
        {
            IEventHandler collecting = new CollectingEventHandler();
            IEventHandler composite = new CompositeEventHandler(collecting);

            Assert.ThrowsAsync<ArgumentNullException>(() => composite.HandleEventAsync(null, CancellationToken.None));
        }

        [Test]
        public async Task HandleEventAsync_MultipleHandlers_HandlersReceiveEvent()
        {
            CollectingEventHandler first = new CollectingEventHandler();
            CollectingEventHandler second = new CollectingEventHandler();
            CompositeEventHandler composite = new CompositeEventHandler(first, second);

            EventDescriptor descriptor = new EventDescriptor
            {
                Timestamp = DateTime.Now,
                Source = NodeType.Engine,
                Severity = EventSeverity.Information,
                EngineId = NodeIdentifier.Generate(NodeType.Engine),
                JobId = "DPJ-001",
                PartitionId = Guid.NewGuid(),
                Description = "xyz"
            };

            await composite.HandleEventAsync(descriptor, CancellationToken.None);

            Assert.That(first.Events.Count, Is.EqualTo(1));
            Assert.That(second.Events.Count, Is.EqualTo(1));
        }


        private class CollectingEventHandler : IEventHandler
        {
            private readonly List<EventDescriptor> _events = new List<EventDescriptor>();


            public IReadOnlyList<EventDescriptor> Events => this._events;


            public Task HandleEventAsync(EventDescriptor descriptor, CancellationToken cancellation)
            {
                this._events.Add(descriptor);

                return Task.CompletedTask;
            }
        }
    }
}
