using DotNetEnv;
using project2_db_benchmark;
using project2_db_benchmark.Benchmarking;

Env.Load();


// Initialize MongoDB helper
Globals.Init();
MongoImportHelper mongoImportHelper = new();
PostgresImportHelper postgresImportHelper = new();

Console.WriteLine("Benchmarking insert in Mongo DB ...");
var (totalTime, throughput, latencies) = await mongoImportHelper.LoadAndInsert();
Console.WriteLine($"Mongo insert took {totalTime} seconds and had an average throughput of {throughput} tuples pr second");
Console.WriteLine($"Average Latency: {latencies.Average():F2} ms");

Console.WriteLine("Benchmarking insert in Postgres ...");
var (postgres_totalTime, postgres_throughput, postgres_latencies) = await postgresImportHelper.LoadAndInsert();
Console.WriteLine($"Postgres insert took {postgres_totalTime} seconds and had an average throughput of {postgres_throughput} tuples pr second");
Console.WriteLine($"Average Latency: {postgres_latencies.Average():F2} ms");
Console.WriteLine($"All data inserted");

