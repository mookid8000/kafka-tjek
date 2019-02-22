using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaTjek
{
    public class KafkaProducer : IDisposable
    {
        static readonly Headers EmptyHeaders = new Headers();

        readonly Producer<string, string> _producer;

        public KafkaProducer(string address)
        {
            var config = new ProducerConfig { BootstrapServers = address};
            
            _producer = new ProducerBuilder<string,string>(config)
                .Build();
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

            ThreadPool.QueueUserWorkItem(_ =>
            {
                _producer.Flush(CancellationToken.None);
                taskCompletionSource.SetResult(null);
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
