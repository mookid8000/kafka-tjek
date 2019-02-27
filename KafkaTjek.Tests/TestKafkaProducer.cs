using System.Threading.Tasks;
using NUnit.Framework;

namespace KafkaTjek.Tests
{
    [TestFixture]
    public class TestKafkaProducer : MyFixtureBase
    {
        KafkaProducer _producer;

        protected override void SetUp()
        {
            Logger.Information("Setting up");

            _producer = new KafkaProducer("localhost:9092");

            Using(_producer);

            Logger.Information("Producer initialized");
        }

        [Test]
        [Ignore("hej")]
        public async Task CanSendEvents()
        {
            Logger.Information("Sending events");

            await Time.Action("produce", async () =>
            {
                await _producer.SendAsync("test-topic",
                    new[]
                    {
                        new KafkaEvent("key1", "hej"),
                        new KafkaEvent("key2", "med"),
                        new KafkaEvent("key2", "dig")
                    });
            });

            Logger.Information("Successfully sent");
        }
    }
}
