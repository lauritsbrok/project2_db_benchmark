using DotNetEnv;
using project2_db_benchmark;
using project2_db_benchmark.Benchmarking;
using project2_db_benchmark.DatabaseHelper;

Env.Load();


// Initialize MongoDB helper
Globals.Init();
MongoImportHelper mongoImportHelper = new();
PostgresImportHelper postgresImportHelper = new();

Console.WriteLine("Inserting into Mongo DB...");
await mongoImportHelper.ImportJsonFiles();
await postgresImportHelper.ImportJsonFiles();
Console.WriteLine($"All data inserted in Mongo DB");