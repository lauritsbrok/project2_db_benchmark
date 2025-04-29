using DotNetEnv;
using project2_db_benchmark;
using project2_db_benchmark.Benchmarking;

Env.Load();


// Initialize MongoDB helper
Globals.Init();
MongoImportHelper mongoImportHelper = new();
PostgresImportHelper postgresImportHelper = new();

Console.WriteLine("Benchmarking insert in Mongo DB ...");
var mongo_insert_benchmark = await mongoImportHelper.BenchmarkInsert();
Console.WriteLine($"Mongo insert took {mongo_insert_benchmark} seconds");
// await postgresImportHelper.ImportJsonFiles();
Console.WriteLine($"All data inserted in Mongo DB");