// using System;
// using System.Diagnostics;
// using System.Threading.Tasks;
// using project2_db_benchmark.mongodb;
// using project2_db_benchmark.postgres;

// namespace project2_db_benchmark
// {
//     public class DatabaseBenchmark
//     {
//         private readonly MongoDatabaseHelper _mongoHelper;
//         private readonly PostgresDatabaseHelper _postgresHelper;
//         private readonly string _yelpDatasetPath;

//         public DatabaseBenchmark(string mongoConnectionString, string yelpDatasetPath)
//         {
//             _mongoHelper = new MongoDatabaseHelper(mongoConnectionString);
//             _postgresHelper = new PostgresDatabaseHelper();
//             _yelpDatasetPath = yelpDatasetPath;
//         }

//         public async Task RunBenchmarkAsync()
//         {
//             Console.WriteLine("Starting database benchmark...");
//             Console.WriteLine("=====================================");

//             // Create tables and indexes for PostgreSQL
//             Console.WriteLine("Setting up PostgreSQL tables and indexes...");
//             await SetupPostgresTablesAsync();

//             // Benchmark data insertion
//             await BenchmarkDataInsertionAsync();

//             // Benchmark query performance
//             await BenchmarkQueryPerformanceAsync();

//             Console.WriteLine("=====================================");
//             Console.WriteLine("Benchmark completed!");
//         }

//         private async Task SetupPostgresTablesAsync()
//         {
//             // Create tables for each entity type
//             string[] tableTypes = { "businesses", "reviews", "users", "tips", "checkins" };

//             foreach (var tableType in tableTypes)
//             {
//                 Console.WriteLine($"Creating table for {tableType}...");
//                 await _postgresHelper.InsertJsonFromFileInChunksAsync(
//                     $"{_yelpDatasetPath}/yelp_academic_dataset_{tableType}.json",
//                     1,
//                     tableType);
//             }
//         }

//         private async Task BenchmarkDataInsertionAsync()
//         {
//             Console.WriteLine("\nBenchmarking data insertion...");
//             Console.WriteLine("=====================================");

//             // Benchmark business data insertion
//             await BenchmarkBusinessInsertionAsync();

//             // Benchmark review data insertion
//             await BenchmarkReviewInsertionAsync();

//             // Benchmark user data insertion
//             await BenchmarkUserInsertionAsync();

//             // Benchmark tip data insertion
//             await BenchmarkTipInsertionAsync();

//             // Benchmark checkin data insertion
//             await BenchmarkCheckinInsertionAsync();
//         }

//         private async Task BenchmarkQueryPerformanceAsync()
//         {
//             Console.WriteLine("\nBenchmarking query performance...");
//             Console.WriteLine("=====================================");

//             // Benchmark business queries
//             await BenchmarkBusinessQueriesAsync();

//             // Benchmark review queries
//             await BenchmarkReviewQueriesAsync();

//             // Benchmark user queries
//             await BenchmarkUserQueriesAsync();

//             // Benchmark tip queries
//             await BenchmarkTipQueriesAsync();

//             // Benchmark checkin queries
//             await BenchmarkCheckinQueriesAsync();
//         }

//         private async Task BenchmarkBusinessInsertionAsync()
//         {
//             string filePath = $"{_yelpDatasetPath}/yelp_academic_dataset_business.json";
//             int chunkSize = 1000;

//             Console.WriteLine("\nBenchmarking business data insertion...");

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "businesses");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             await _mongoHelper.ImportBusinessesFromFileAsync(filePath, chunkSize);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }

//         private async Task BenchmarkReviewInsertionAsync()
//         {
//             string filePath = $"{_yelpDatasetPath}/yelp_academic_dataset_review.json";
//             int chunkSize = 1000;

//             Console.WriteLine("\nBenchmarking review data insertion...");

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "reviews");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             await _mongoHelper.ImportReviewsFromFileAsync(filePath, chunkSize);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }

//         private async Task BenchmarkUserInsertionAsync()
//         {
//             string filePath = $"{_yelpDatasetPath}/yelp_academic_dataset_user.json";
//             int chunkSize = 1000;

//             Console.WriteLine("\nBenchmarking user data insertion...");

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "users");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             await _mongoHelper.ImportUsersFromFileAsync(filePath, chunkSize);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }

//         private async Task BenchmarkTipInsertionAsync()
//         {
//             string filePath = $"{_yelpDatasetPath}/yelp_academic_dataset_tip.json";
//             int chunkSize = 1000;

//             Console.WriteLine("\nBenchmarking tip data insertion...");

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "tips");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             await _mongoHelper.ImportTipsFromFileAsync(filePath, chunkSize);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }

//         private async Task BenchmarkCheckinInsertionAsync()
//         {
//             string filePath = $"{_yelpDatasetPath}/yelp_academic_dataset_checkin.json";
//             int chunkSize = 1000;

//             Console.WriteLine("\nBenchmarking checkin data insertion...");

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "checkins");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             await _mongoHelper.ImportCheckinsFromFileAsync(filePath, chunkSize);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }

//         private async Task BenchmarkBusinessQueriesAsync()
//         {
//             Console.WriteLine("\nBenchmarking business queries...");

//             // Query 1: Find businesses by rating
//             Console.WriteLine("Query 1: Find businesses by rating (stars > 4.0)");

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             var postgresResults = await _postgresHelper.ExecuteQueryAsync(
//                 "SELECT * FROM businesses WHERE stars > 4.0 LIMIT 100");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             var mongoResults = await _mongoHelper.FindBusinessesByRatingAsync(4.0, 100);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms, {postgresResults.Count} results");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms, {mongoResults.Count} results");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");

//             // Query 2: Find businesses by location
//             Console.WriteLine("\nQuery 2: Find businesses by location (within 5km of a point)");

//             // PostgreSQL
//             postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             postgresResults = await _postgresHelper.ExecuteQueryAsync(
//                 "SELECT * FROM businesses WHERE earth_box(ll_to_earth(37.7749, -122.4194), 5000) @> ll_to_earth(latitude, longitude) LIMIT 100");
//             postgresStopwatch.Stop();

//             // MongoDB
//             mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             mongoResults = await _mongoHelper.FindBusinessesByLocationAsync(-122.4194, 37.7749, 5000, 100);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms, {postgresResults.Count} results");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms, {mongoResults.Count} results");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }

//         private async Task BenchmarkReviewQueriesAsync()
//         {
//             Console.WriteLine("\nBenchmarking review queries...");

//             // Query 1: Find reviews by business ID
//             Console.WriteLine("Query 1: Find reviews by business ID");

//             // Get a sample business ID
//             var sampleBusiness = await _postgresHelper.ExecuteQueryAsync("SELECT business_id FROM businesses LIMIT 1");
//             if (sampleBusiness.Count == 0)
//             {
//                 Console.WriteLine("No businesses found in the database.");
//                 return;
//             }

//             string businessId = sampleBusiness[0]["business_id"].ToString();

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             var postgresResults = await _postgresHelper.ExecuteQueryAsync(
//                 $"SELECT * FROM reviews WHERE business_id = '{businessId}' LIMIT 100");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             var mongoResults = await _mongoHelper.FindReviewsByBusinessIdAsync(businessId, 100);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms, {postgresResults.Count} results");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms, {mongoResults.Count} results");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }

//         private async Task BenchmarkUserQueriesAsync()
//         {
//             Console.WriteLine("\nBenchmarking user queries...");

//             // Query 1: Find users by name
//             Console.WriteLine("Query 1: Find users by name (contains 'John')");

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             var postgresResults = await _postgresHelper.ExecuteQueryAsync(
//                 "SELECT * FROM users WHERE name LIKE '%John%' LIMIT 100");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             var mongoResults = await _mongoHelper.FindUsersByNameAsync("John", 100);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms, {postgresResults.Count} results");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms, {mongoResults.Count} results");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }

//         private async Task BenchmarkTipQueriesAsync()
//         {
//             Console.WriteLine("\nBenchmarking tip queries...");

//             // Query 1: Find tips by business ID
//             Console.WriteLine("Query 1: Find tips by business ID");

//             // Get a sample business ID
//             var sampleBusiness = await _postgresHelper.ExecuteQueryAsync("SELECT business_id FROM businesses LIMIT 1");
//             if (sampleBusiness.Count == 0)
//             {
//                 Console.WriteLine("No businesses found in the database.");
//                 return;
//             }

//             string businessId = sampleBusiness[0]["business_id"].ToString();

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             var postgresResults = await _postgresHelper.ExecuteQueryAsync(
//                 $"SELECT * FROM tips WHERE business_id = '{businessId}' LIMIT 100");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             // Assuming there's a method to find tips by business ID in MongoDB
//             // If not, you'll need to implement it
//             var mongoResults = await _mongoHelper.FindTipsByBusinessIdAsync(businessId, 100);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms, {postgresResults.Count} results");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms, {mongoResults.Count} results");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }

//         private async Task BenchmarkCheckinQueriesAsync()
//         {
//             Console.WriteLine("\nBenchmarking checkin queries...");

//             // Query 1: Find checkins by business ID
//             Console.WriteLine("Query 1: Find checkins by business ID");

//             // Get a sample business ID
//             var sampleBusiness = await _postgresHelper.ExecuteQueryAsync("SELECT business_id FROM businesses LIMIT 1");
//             if (sampleBusiness.Count == 0)
//             {
//                 Console.WriteLine("No businesses found in the database.");
//                 return;
//             }

//             string businessId = sampleBusiness[0]["business_id"].ToString();

//             // PostgreSQL
//             var postgresStopwatch = new Stopwatch();
//             postgresStopwatch.Start();
//             var postgresResults = await _postgresHelper.ExecuteQueryAsync(
//                 $"SELECT * FROM checkins WHERE business_id = '{businessId}' LIMIT 100");
//             postgresStopwatch.Stop();

//             // MongoDB
//             var mongoStopwatch = new Stopwatch();
//             mongoStopwatch.Start();
//             var mongoResults = await _mongoHelper.FindCheckinsByBusinessIdAsync(
//                 businessId,
//                 DateTime.Now.AddDays(-30),
//                 DateTime.Now,
//                 100);
//             mongoStopwatch.Stop();

//             Console.WriteLine($"PostgreSQL: {postgresStopwatch.ElapsedMilliseconds}ms, {postgresResults.Count} results");
//             Console.WriteLine($"MongoDB: {mongoStopwatch.ElapsedMilliseconds}ms, {mongoResults.Count} results");
//             Console.WriteLine($"Difference: {Math.Abs(postgresStopwatch.ElapsedMilliseconds - mongoStopwatch.ElapsedMilliseconds)}ms");
//         }
//     }
// }