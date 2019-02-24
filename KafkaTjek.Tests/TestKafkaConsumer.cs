using System.Collections.Concurrent;
using System.Threading.Tasks;
using KafkaTjek.Tests.Extensions;
using NUnit.Framework;
// ReSharper disable ArgumentsStyleAnonymousFunction
#pragma warning disable 1998

namespace KafkaTjek.Tests
{
    [TestFixture]
    public class TestKafkaConsumer : MyFixtureBase
    {
        KafkaConsumer _consumer;

        [Test]
        public async Task CanConsumeEvent()
        {
            var receivedEvents = new ConcurrentQueue<string>();

            _consumer = new KafkaConsumer(
                address: "localhost:9092",
                topics: new[] { "test-topic" },
                group: "default4",
                eventHandler: async evt => receivedEvents.Enqueue(evt.Body)
            );

            Using(_consumer);

            _consumer.Start();

            await receivedEvents.WaitOrDie(q => q.Count == 3, timeoutSeconds: 7);
        }
    }
}