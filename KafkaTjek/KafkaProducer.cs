﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;
using static KafkaTjek.Internals.Handlers;
// ReSharper disable MethodSupportsCancellation

namespace KafkaTjek
{
    public class KafkaProducer : IDisposable
    {
        static readonly ILogger Logger = Log.ForContext<KafkaProducer>();
        static readonly Headers EmptyHeaders = new Headers();

        readonly Producer<string, string> _producer;
        readonly int _sendTimeoutSeconds;

        public KafkaProducer(string address, int sendTimeoutSeconds = 30)
        {
            _sendTimeoutSeconds = sendTimeoutSeconds;
            var config = new ProducerConfig { BootstrapServers = address };

            _producer = new ProducerBuilder<string, string>(config)
                .SetLogHandler((producer, message) => LogHandler(Logger, producer, message))
                .SetErrorHandler((producer, message) => ErrorHandler(Logger, producer, message))
                .Build();

            Logger.Information("Kafka producer initialized with {address}", address);
        }

        public Task SendAsync(string topic, IEnumerable<KafkaEvent> events)
        {
            var taskCompletionSource = new TaskCompletionSource<object>();

            foreach (var evt in events)
            {
                var message = new Message<string, string>
                {
                    Key = evt.Key,
                    Headers = GetHeaders(evt.Headers),
                    Value = evt.Body
                };
                _producer.BeginProduce(topic, message);
            }

            var cancellationTokenSource = new CancellationTokenSource();

            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(_sendTimeoutSeconds));

            var cancellationToken = cancellationTokenSource.Token;

            ThreadPool.QueueUserWorkItem(_ =>
            {
                try
                {
                    _producer.Flush(cancellationToken);

                    if (!cancellationToken.IsCancellationRequested)
                    {
                        taskCompletionSource.SetResult(null);
                    }
                    else
                    {
                        taskCompletionSource.SetException(
                            new TimeoutException($"Could not send events within {_sendTimeoutSeconds} s timeout"));
                    }
                }
                catch (OperationCanceledException exception) when (cancellationToken.IsCancellationRequested)
                {
                    taskCompletionSource.SetException(
                        new TimeoutException($"Could not send events within {_sendTimeoutSeconds} s timeout", exception));
                }
                catch (Exception exception)
                {
                    taskCompletionSource.SetException(exception);
                }
                finally
                {
                    cancellationTokenSource.Dispose();
                }
            });

            return taskCompletionSource.Task;
        }

        static Headers GetHeaders(Dictionary<string, string> dictionary)
        {
            if (dictionary.Count == 0) return EmptyHeaders;

            var headers = new Headers();

            foreach (var (key, value) in dictionary)
            {
                headers.Add(key, Encoding.UTF8.GetBytes(value));
            }

            return headers;
        }

        public void Dispose() => _producer.Dispose();
    }
}
