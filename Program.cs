using DotNetEnv;
using project2_db_benchmark;
using project2_db_benchmark.Benchmarking;

Env.Load();


// Initialize MongoDB helper
Globals.Init();
MongoImportHelper mongoImportHelper = new();
PostgresImportHelper postgresImportHelper = new();

Console.WriteLine("Benchmarking insert in Mongo DB ...");
var (totalTime, throughput, latencies) = await mongoImportHelper.BenchmarkInsert();
Console.WriteLine($"Mongo insert took {totalTime} seconds and had an average throughput of {throughput} tuples pr second");
Console.WriteLine($"Average Latency: {latencies.Average():F2} ms");

Console.WriteLine("Benchmarking insert in Postgres ...");
var postgres_insert_benchmark = await postgresImportHelper.LoadAndInsert();
Console.WriteLine($"All data inserted in Mongo DB");

