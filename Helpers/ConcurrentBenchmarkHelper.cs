using System.Collections.Concurrent;

namespace project2_db_benchmark.Helpers;

public static class ConcurrentBenchmarkHelper{

    private static readonly SemaphoreSlim _semaphore = new (8);
    public static async Task<(double totalTime, List<double> latencies)> RunTasks(List<Func<Task>> tasks){

    
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