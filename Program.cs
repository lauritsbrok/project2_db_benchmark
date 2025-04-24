using DotNetEnv;
using project2_db_benchmark;
using project2_db_benchmark.Benchmarking;
using project2_db_benchmark.DatabaseHelper;

Env.Load();


// Initialize MongoDB helper
Globals.Init();
PostgresDatabaseHelper postgresHelper = new();
MongoImportHelper mongoImportHelper = new();

Console.WriteLine("Inserting into Mongo DB...");
await mongoImportHelper.ImportJsonFiles();
Console.WriteLine($"All data inserted in Mongo DB");