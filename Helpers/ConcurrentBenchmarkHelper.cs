using System.Collections.Concurrent;

namespace project2_db_benchmark.Helpers;

public static class ConcurrentBenchmarkHelper
{

    private static SemaphoreSlim _semaphore = new(1); // Default to 1 for safety

    public static async Task<(double totalTime, List<double> latencies)> RunTasks(List<Func<Task>> tasks, int concurrencyLevel = 8)
    {
        // Reinitialize semaphore with desired concurrency level
        _semaphore = new SemaphoreSlim(concurrencyLevel);

        var latencies = new ConcurrentBag<double>();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var tasklist = tasks.Select(async task =>
        {
            await _semaphore.WaitAsync();
            var localWatch = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                await task();
            }
            finally
            {
                localWatch.Stop();
                latencies.Add(localWatch.Elapsed.TotalMilliseconds); // Or TotalSeconds
                _semaphore.Release();
            }
        });

        await Task.WhenAll(tasklist);

        stopwatch.Stop();

        return (stopwatch.Elapsed.TotalSeconds, latencies.ToList());
    }
}