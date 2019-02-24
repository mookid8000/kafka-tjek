using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaTjek.Tests.Extensions
{
    static class TestExtensions
    {
        public static async Task WaitOrDie<T>(this ConcurrentQueue<T> queue,
            Expression<Func<ConcurrentQueue<T>, bool>> completionExpression, int timeoutSeconds = 5)
        {
            var completionPredicate = completionExpression.Compile();
            var cancellationTokenSource = new CancellationTokenSource();

            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

            var cancellationToken = cancellationTokenSource.Token;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    if (completionPredicate(queue)) return;

                    await Task.Delay(117, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationTokenSource.IsCancellationRequested)
            {
            }

            throw new TimeoutException($@"Waiting for

    {completionExpression}

on queue did not complete in {timeoutSeconds} s");
        }
    }
}