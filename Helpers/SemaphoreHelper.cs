using MongoDB.Driver;

namespace project2_db_benchmark.Helpers;

public static class SemaphoreHelper{

    private static readonly SemaphoreSlim _semaphore = new (8);
    public static async Task<double> GetTasks(List<Func<Task>> inserts){

    
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();


        var tasks = inserts.Select(async insert => 
        {
            await _semaphore.WaitAsync();
            try
            {
                await insert();
            }
            finally
            {
                _semaphore.Release();
            }
        });

        await Task.WhenAll(tasks);

        stopwatch.Stop();
    
        return stopwatch.Elapsed.TotalSeconds;
    }
}