using System.Threading.Tasks;
using NUnit.Framework;
using Testy;

namespace KafkaTjek.Tests
{
    [TestFixture]
    public class TestKafkaProducer : FixtureBase
    {
        KafkaProducer _producer;

        protected override void SetUp()
        {
            _producer = new KafkaProducer("localhost:9092");

            Using(_producer);
        }

        [Test]
        public async Task CanSendEvents()
        {
            await _producer.SendAsync("test-topic",
                new[]
                {
                    new KafkaEvent("key1", "hej"),
                    new KafkaEvent("key2", "med"),
                    new KafkaEvent("key2", "dig")
                });
        }
    }
}
