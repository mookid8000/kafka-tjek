using Serilog;
using Testy;

namespace KafkaTjek.Tests
{
    public abstract class MyFixtureBase : FixtureBase
    {
        static MyFixtureBase()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .MinimumLevel.Verbose()
                .CreateLogger();
        }

        protected ILogger Logger => Log.ForContext("SourceContext", GetType());
    }
}