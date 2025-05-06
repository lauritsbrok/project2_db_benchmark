using DotNetEnv;
using project2_db_benchmark;
using project2_db_benchmark.Benchmarking;
using project2_db_benchmark.Models;

Env.Load();

// Parse command line arguments
int numConcurrent = 8; // Default value
string database = "both"; // Default to running both databases
string benchmarkType = "write"; // Default to read
var cmdArgs = Environment.GetCommandLineArgs();
string resultsDir = "results";
for (int i = 0; i < cmdArgs.Length - 1; i++)
{
    if (cmdArgs[i] == "--num_concurrent" && int.TryParse(cmdArgs[i + 1], out int parsedValue))
    {
        numConcurrent = parsedValue;
    }
    else if (cmdArgs[i] == "--database" && i + 1 < cmdArgs.Length)
    {
        database = cmdArgs[i + 1].ToLower();
        if (database != "mongo" && database != "postgres" && database != "both")
        {
            Console.WriteLine("Invalid database option. Use 'mongo', 'postgres', or 'both'");
            return;
        }
    }
    else if (cmdArgs[i] == "--benchmark_type" && i + 1 < cmdArgs.Length)
    {
        benchmarkType = cmdArgs[i + 1].ToLower();
        if (benchmarkType != "user-read" && benchmarkType != "write" && benchmarkType != "full-table-read" && benchmarkType != "user-story")
        {
            Console.WriteLine("Invalid benchmark type option. Use 'user-read', 'write', 'full-table-read', or 'user-story'");
            return;
        }
    }
    else if (cmdArgs[i] == "--results_dir")
    {
        resultsDir = cmdArgs[i + 1];
    }
}

Globals.Init();
MongoBenchmarkHelper mongoBenchmarkHelper = new();
MongoBenchmarkHelper postgresBenchmarkHelper = new();
UserStoryBenchmarkHelper userStoryBenchmarkHelper = new();

if (benchmarkType == "write")
{
    Console.WriteLine($"Running with concurrency level: {numConcurrent}");
    Console.WriteLine($"Selected database(s): {database}");

    // Initialize helpers
    Console.WriteLine("Initializing helpers...");
    Console.WriteLine("Helpers initialized");

    if (database == "mongo" || database == "both")
    {
        string resultsFileMongo = $"{resultsDir}/mongo_write.csv";
        Console.WriteLine("Benchmarking insert in Mongo DB ...");
        var (totalTime, throughput, latencies) = await mongoBenchmarkHelper.LoadAndInsert(numConcurrent);
        File.AppendAllText(resultsFileMongo, $"{numConcurrent},{totalTime},{throughput},{latencies.Min()},{latencies.Max()},{latencies.Average()},{latencies.OrderBy(l => l).ElementAt(latencies.Count / 100 * 99)},{latencies.OrderBy(l => l).ElementAt(latencies.Count / 100 * 90)}\n");
    }

    if (database == "postgres" || database == "both")
    {
        string resultsFilePostgres = $"{resultsDir}/postgres_write.csv";
        Console.WriteLine("Benchmarking insert in Postgres ...");
        var (totalTime, throughput, latencies) = await postgresBenchmarkHelper.LoadAndInsert(numConcurrent);
        File.AppendAllText(resultsFilePostgres, $"{numConcurrent},{totalTime},{throughput},{latencies.Min()},{latencies.Max()},{latencies.Average()},{latencies.OrderBy(l => l).ElementAt(latencies.Count / 100 * 99)},{latencies.OrderBy(l => l).ElementAt(latencies.Count / 100 * 90)}\n");
    }
}

if (benchmarkType == "user-read")
{
    if (database == "mongo" || database == "both")
    {
        Console.WriteLine("\nBenchmarking user reads in Mongo DB ...");
        string resultsFileMongo = $"{resultsDir}/mongo_user_read.csv";
        var (mongo_read_totalTime, mongo_read_throughput, mongo_read_latencies) = await mongoBenchmarkHelper.BenchmarkReads(numConcurrent);
        File.AppendAllText(resultsFileMongo, $"{numConcurrent},{mongo_read_totalTime},{mongo_read_throughput},{mongo_read_latencies.Min()},{mongo_read_latencies.Max()},{mongo_read_latencies.Average()},{mongo_read_latencies.OrderBy(l => l).ElementAt(mongo_read_latencies.Count / 100 * 99)},{mongo_read_latencies.OrderBy(l => l).ElementAt(mongo_read_latencies.Count / 100 * 90)}\n");
    }
    if (database == "postgres" || database == "both")
    {
        Console.WriteLine("Benchmarking user reads in Postgres ...");
        string resultsFilePostgres = $"{resultsDir}/postgres_user_read.csv";
        var (postgres_read_totalTime, postgres_read_throughput, postgres_read_latencies) = await postgresBenchmarkHelper.BenchmarkReads(numConcurrent);
        File.AppendAllText(resultsFilePostgres, $"{numConcurrent},{postgres_read_totalTime},{postgres_read_throughput},{postgres_read_latencies.Min()},{postgres_read_latencies.Max()},{postgres_read_latencies.Average()},{postgres_read_latencies.OrderBy(l => l).ElementAt(postgres_read_latencies.Count / 100 * 99)},{postgres_read_latencies.OrderBy(l => l).ElementAt(postgres_read_latencies.Count / 100 * 90)}\n");
    }

}

if (benchmarkType == "full-table-read")
{
    if (database == "mongo" || database == "both")
    {
        Console.WriteLine("\nBenchmarking full table reads in Mongo DB ...");
        string resultsFileMongo = $"{resultsDir}/mongo_full_table_read.csv";
        var (mongo_read_totalTime, mongo_read_throughput, mongo_read_latencies) = await mongoBenchmarkHelper.BenchmarkFullCollectionReads(numConcurrent);
        File.AppendAllText(resultsFileMongo, $"{numConcurrent},{mongo_read_totalTime},{mongo_read_throughput},{mongo_read_latencies.Min()},{mongo_read_latencies.Max()},{mongo_read_latencies.Average()},{mongo_read_latencies.OrderBy(l => l).ElementAt(mongo_read_latencies.Count / 100 * 99)},{mongo_read_latencies.OrderBy(l => l).ElementAt(mongo_read_latencies.Count / 100 * 90)}\n");
    }
    if (database == "postgres" || database == "both")
    {
        Console.WriteLine("Benchmarking full table reads in Postgres ...");
        string resultsFilePostgres = $"{resultsDir}/postgres_full_table_read.csv";
        var (postgres_read_totalTime, postgres_read_throughput, postgres_read_latencies) = await postgresBenchmarkHelper.BenchmarkFullCollectionReads(numConcurrent);
        File.AppendAllText(resultsFilePostgres, $"{numConcurrent},{postgres_read_totalTime},{postgres_read_throughput},{postgres_read_latencies.Min()},{postgres_read_latencies.Max()},{postgres_read_latencies.Average()},{postgres_read_latencies.OrderBy(l => l).ElementAt(postgres_read_latencies.Count / 100 * 99)},{postgres_read_latencies.OrderBy(l => l).ElementAt(postgres_read_latencies.Count / 100 * 90)}\n");
    }

}

if (benchmarkType == "user-story")
{
    if (database == "mongo" || database == "both")
    {
        Console.WriteLine("\nBenchmarking user story in Mongo DB ...");
        string resultsFileMongo = $"{resultsDir}/mongo_user_story.csv";
        var (mongo_read_totalTime, mongo_read_throughput, mongo_read_latencies) = await userStoryBenchmarkHelper.RunBenchmarkAsyncMongo(numConcurrent);
        File.AppendAllText(resultsFileMongo, $"{numConcurrent},{mongo_read_totalTime},{mongo_read_throughput},{mongo_read_latencies.Min()},{mongo_read_latencies.Max()},{mongo_read_latencies.Average()},{mongo_read_latencies.OrderBy(l => l).ElementAt(mongo_read_latencies.Count / 100 * 99)},{mongo_read_latencies.OrderBy(l => l).ElementAt(mongo_read_latencies.Count / 100 * 90)}\n");
    }
    if (database == "postgres" || database == "both")
    {
        Console.WriteLine("Benchmarking user story in Postgres ...");
        string resultsFilePostgres = $"{resultsDir}/postgres_user_story.csv";
        var (postgres_read_totalTime, postgres_read_throughput, postgres_read_latencies) = await userStoryBenchmarkHelper.RunBenchmarkAsyncPostgres(numConcurrent);
        File.AppendAllText(resultsFilePostgres, $"{numConcurrent},{postgres_read_totalTime},{postgres_read_throughput},{postgres_read_latencies.Min()},{postgres_read_latencies.Max()},{postgres_read_latencies.Average()},{postgres_read_latencies.OrderBy(l => l).ElementAt(postgres_read_latencies.Count / 100 * 99)},{postgres_read_latencies.OrderBy(l => l).ElementAt(postgres_read_latencies.Count / 100 * 90)}\n");
    }
}