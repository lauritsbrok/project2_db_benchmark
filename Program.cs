using DotNetEnv;
using project2_db_benchmark;
using project2_db_benchmark.Benchmarking;

Env.Load();


// Initialize MongoDB helper
Globals.Init();
MongoImportHelper mongoImportHelper = new();
PostgresImportHelper postgresImportHelper = new();

Console.WriteLine("Benchmarking insert in Mongo DB ...");
var mongo_insert_benchmark = await mongoImportHelper.LoadAndInsert();
Console.WriteLine($"Mongo insert took {mongo_insert_benchmark} seconds");

Console.WriteLine("Benchmarking insert in Postgres DB ...");
var postgres_insert_benchmark = await postgresImportHelper.LoadAndInsert();
Console.WriteLine($"Postgres insert took {postgres_insert_benchmark} seconds");


