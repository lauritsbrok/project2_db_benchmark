using DotNetEnv;
using project2_db_benchmark;
using project2_db_benchmark.Benchmarking;

Env.Load();

// Parse command line arguments
int numConcurrent = 8; // Default value
string database = "both"; // Default to running both databases
string benchmarkType = "write"; // Default to read
var cmdArgs = Environment.GetCommandLineArgs();
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
        if (benchmarkType != "read" && benchmarkType != "write")
        {
            Console.WriteLine("Invalid benchmark type option. Use 'read' or 'write'");
            return;
        }
    }
}

if (benchmarkType == "write")
{
    Console.WriteLine($"Running with concurrency level: {numConcurrent}");
    Console.WriteLine($"Selected database(s): {database}");

    // Initialize helpers
    Globals.Init();
    Console.WriteLine("Initializing helpers...");
    MongoImportHelper mongoImportHelper = new();
    PostgresImportHelper postgresImportHelper = new();
    Console.WriteLine("Helpers initialized");

    if (database == "mongo" || database == "both")
    {
        string resultsFileMongo = $"results/mongo.csv";
        Console.WriteLine("Benchmarking insert in Mongo DB ...");
        var (totalTime, throughput, latencies) = await mongoImportHelper.LoadAndInsert(numConcurrent);
        File.AppendAllText(resultsFileMongo, $"{numConcurrent},{totalTime},{throughput},{latencies.Min()},{latencies.Max()},{latencies.Average()},{latencies.OrderBy(l => l).ElementAt(latencies.Count / 100 * 99)},{latencies.OrderBy(l => l).ElementAt(latencies.Count / 100 * 90)}\n");
    }

    if (database == "postgres" || database == "both")
    {
        string resultsFilePostgres = $"results/postgres.csv";
        Console.WriteLine("Benchmarking insert in Postgres ...");
        var (totalTime, throughput, latencies) = await postgresImportHelper.LoadAndInsert(numConcurrent);
        File.AppendAllText(resultsFilePostgres, $"{numConcurrent},{totalTime},{throughput},{latencies.Min()},{latencies.Max()},{latencies.Average()},{latencies.OrderBy(l => l).ElementAt(latencies.Count / 100 * 99)},{latencies.OrderBy(l => l).ElementAt(latencies.Count / 100 * 90)}\n");
    }
}

