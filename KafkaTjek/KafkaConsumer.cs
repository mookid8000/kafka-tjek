﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;
using static KafkaTjek.Internals.Handlers;
// ReSharper disable RedundantAnonymousTypePropertyName

namespace KafkaTjek
{
    public class KafkaConsumer : IDisposable
    {
        static readonly ILogger Logger = Log.ForContext<KafkaConsumer>();
        readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        readonly string _group;
        readonly Func<KafkaEvent, Task> _eventHandler;
        readonly Consumer<string, string> _consumer;
        readonly Thread _worker;

        public KafkaConsumer(string address, IEnumerable<string> topics, string group, Func<KafkaEvent, Task> eventHandler)
        {
            _group = group ?? throw new ArgumentNullException(nameof(@group));
            _eventHandler = eventHandler ?? throw new ArgumentNullException(nameof(eventHandler));

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = address,
                GroupId = group,

                AutoOffsetReset = AutoOffsetReset.Earliest,
                AutoCommitIntervalMs = 2000,
                EnableAutoCommit = true
            };

            _consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetLogHandler((consumer, message) => LogHandler(Logger, consumer, message))
                .SetErrorHandler((consumer, error) => ErrorHandler(Logger, consumer, error))
                .SetRebalanceHandler((consumer, rebalanceEvent) => RebalanceHandler(Logger, consumer, rebalanceEvent))
                .SetOffsetsCommittedHandler((consumer, committedOffsets) => OffsetsCommitted(Logger, consumer, committedOffsets))
                .Build();

            var topicsToSubscribeTo = new HashSet<string>(topics);

            Logger.Information("Kafka consumer for group {consumerGroup} subscribing to topics: {@topics}", _group, topicsToSubscribeTo);

            foreach (var topic in topicsToSubscribeTo)
            {
                _consumer.Subscribe(topic);
            }

            _worker = new Thread(Run) { IsBackground = true };
        }

        public void Start()
        {
            if (_worker.ThreadState == ThreadState.Running)
            {
                throw new InvalidOperationException("Kafka consumer worker is already running");
            }
            _worker.Start();
        }

        void Run()
        {
            var cancellationToken = _cancellationTokenSource.Token;

            Logger.Information("Starting Kafka consumer worker for group {consumerGroup}", _group);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(cancellationToken);

                        var kafkaEvent = new KafkaEvent(
                            consumeResult.Key,
                            consumeResult.Value,
                            GetHeaders(consumeResult.Headers)
                        );

                        var topf = consumeResult.TopicPartitionOffset;
                        
                        Logger.Verbose("Received event: {@event} - {@position}", kafkaEvent, new { Topic = topf.Topic, Offset = $"{topf.Partition.Value}/{topf.Offset.Value}" });

                        _eventHandler(kafkaEvent).Wait(cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                    }
                    catch (ThreadAbortException)
                    {
                        Logger.Warning("Kafka consumer worker aborted!");
                        return;
                    }
                    catch (Exception exception)
                    {
                        Logger.Warning(exception, "Unhandled exception in Kafka consumer loop");

                        try
                        {
                            Task.Delay(TimeSpan.FromSeconds(30), cancellationToken)
                                .Wait(cancellationToken);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                        }
                    }
                }
            }
            catch (Exception exception)
            {
                Logger.Fatal(exception, "Unhandled exception in Kafka consumer");
            }
            finally
            {
                Logger.Information("Kafka consumer worker for group {consumerGroup} stopped", _group);
            }
        }

        static Dictionary<string, string> GetHeaders(Headers headers)
        {
            var dictionary = new Dictionary<string, string>();

            foreach (var kvp in headers)
            {
                dictionary[kvp.Key] = Encoding.UTF8.GetString(kvp.Value);
            }

            return dictionary;
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();

            using (_consumer)
            using (_cancellationTokenSource)
            {
                if (!_worker.Join(TimeSpan.FromSeconds(5)))
                {
                    Logger.Error("Kafka consumer worker did not finish executing within 5 s");

                    _worker.Abort();
                }
            }
        }
    }
}