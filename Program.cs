using DotNetEnv;
using project2_db_benchmark;
using project2_db_benchmark.Benchmarking;

Env.Load();


// Initialize MongoDB helper
Globals.Init();
MongoBenchmarkHelper mongoImportHelper = new();
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

// Benchmark read operations
Console.WriteLine("\nBenchmarking reads in Mongo DB ...");
var (mongo_read_totalTime, mongo_read_throughput, mongo_read_latencies) = await mongoImportHelper.BenchmarkReads();
Console.WriteLine($"Mongo reads took {mongo_read_totalTime} seconds and had an average throughput of {mongo_read_throughput} reads pr second");
Console.WriteLine($"Average Read Latency: {mongo_read_latencies.Average():F2} ms");

Console.WriteLine("\nBenchmarking reads in Postgres ...");
var (postgres_read_totalTime, postgres_read_throughput, postgres_read_latencies) = await postgresImportHelper.BenchmarkReads();
Console.WriteLine($"Postgres reads took {postgres_read_totalTime} seconds and had an average throughput of {postgres_read_throughput} reads pr second");
Console.WriteLine($"Average Read Latency: {postgres_read_latencies.Average():F2} ms");

// Benchmark full collection/table reads
Console.WriteLine("\nBenchmarking full collection reads in MongoDB ...");
var (mongo_full_read_totalTime, mongo_full_read_throughput, mongo_full_read_latencies) = await mongoImportHelper.BenchmarkFullCollectionReads();
Console.WriteLine($"MongoDB full collection reads took {mongo_full_read_totalTime} seconds and had an average throughput of {mongo_full_read_throughput} reads pr second");
Console.WriteLine($"Average Full Collection Read Latency: {mongo_full_read_latencies.Average():F2} ms");

Console.WriteLine("\nBenchmarking full table reads in PostgreSQL ...");
var (postgres_full_read_totalTime, postgres_full_read_throughput, postgres_full_read_latencies) = await postgresImportHelper.BenchmarkFullTableReads();
Console.WriteLine($"PostgreSQL full table reads took {postgres_full_read_totalTime} seconds and had an average throughput of {postgres_full_read_throughput} reads pr second");
Console.WriteLine($"Average Full Table Read Latency: {postgres_full_read_latencies.Average():F2} ms");

