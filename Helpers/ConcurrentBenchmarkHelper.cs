namespace project2_db_benchmark.Helpers;

public static class ConcurrentBenchmarkHelper{

    private static readonly SemaphoreSlim _semaphore = new (8);
    public static async Task<double> RunTasks(List<Func<Task>> tasks){

    
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();


        var tasklist = tasks.Select(async task => 
        {
            await _semaphore.WaitAsync();
            try
            {
                await task();
            }
            finally
            {
                _semaphore.Release();
            }
        });

        await Task.WhenAll(tasklist);

        stopwatch.Stop();
    
        return stopwatch.Elapsed.TotalSeconds;
    }
}