﻿using System.Linq;
using System.Threading.Tasks;
using KafkaTjek.Tests.Extensions;
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

        [TestCase(10000)]
        [Ignore("hej")]
        public async Task CanSendEvents_Lots(int count)
        {
            Logger.Information("Sending events");

            await Time.Action("produce", async () =>
            {
                var messages = Enumerable.Range(0, count)
                    .Select(n => new KafkaEvent($"key-{n % 64}", $"det her er besked nr {n}"));

                foreach (var batch in messages.Batch(100))
                {
                    await _producer.SendAsync("lots", batch);
                }
            });

            Logger.Information("Successfully sent");
        }
    }
}
