using System;
using System.Diagnostics;
using System.Threading.Tasks;
using project2_db_benchmark.postgres;

namespace project2_db_benchmark
{
    public class PostgresTest
    {
        private readonly PostgresDatabaseHelper _postgresHelper;
        private readonly string _yelpDatasetPath;

        public PostgresTest(string yelpDatasetPath)
        {
            _postgresHelper = new PostgresDatabaseHelper();
            _yelpDatasetPath = yelpDatasetPath;
        }

        public async Task RunPostgresTestAsync()
        {
            Console.WriteLine("Starting PostgreSQL test...");
            Console.WriteLine("=====================================");

            // Test database connection
            await TestConnectionAsync();

            await AddExtension();

            // Test table creation
            await TestTableCreationAsync();

            // Test data insertion
            await TestDataInsertionAsync();

            // Test query performance
            await TestQueryPerformanceAsync();

            Console.WriteLine("=====================================");
            Console.WriteLine("PostgreSQL test completed!");
        }

        private async Task TestConnectionAsync()
        {
            Console.WriteLine("\nTesting PostgreSQL connection...");
            try
            {
                // Try to execute a simple query to test the connection
                var result = await _postgresHelper.ExecuteQueryAsync("SELECT 1 as test");
                Console.WriteLine("PostgreSQL connection successful!");

            }
            catch (Exception ex)
            {
                Console.WriteLine($"PostgreSQL connection failed: {ex.Message}");
                throw;
            }
        }

        private async Task AddExtension()
        {
            Console.WriteLine("\nAdding extension ...");
            try
            {
                // Try to execute a simple query to test the connection
                var result = await _postgresHelper.ExecuteQueryAsync("CREATE EXTENSION IF NOT EXISTS cube; CREATE EXTENSION IF NOT EXISTS earthdistance;");

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Extension creation failes: {ex.Message}");
                throw;
            }
        }

        private async Task TestTableCreationAsync()
        {
            Console.WriteLine("\nTesting PostgreSQL table creation...");

            try
            {
                // Explicitly create all tables
                await _postgresHelper.CreateAllTablesAsync();
                Console.WriteLine("All tables created successfully.");

                // Setup indexes after tables are created
                Console.WriteLine("\nSetting up indexes...");
                await _postgresHelper.SetupIndexesAsync();
                Console.WriteLine("Indexes created successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to create tables or indexes: {ex.Message}");
            }
        }

        private async Task TestDataInsertionAsync()
        {
            Console.WriteLine("\nTesting PostgreSQL data insertion...");
            Console.WriteLine("=====================================");

            // Test business data insertion
            await TestBusinessInsertionAsync();

            // Test review data insertion
            await TestReviewInsertionAsync();

            // Test user data insertion
            await TestUserInsertionAsync();

            // Test tip data insertion
            await TestTipInsertionAsync();

            // Test checkin data insertion
            await TestCheckinInsertionAsync();
        }

        private async Task TestBusinessInsertionAsync()
        {
            string filePath = $"{_yelpDatasetPath}/business_reduced.json";
            int chunkSize = 100; // Smaller chunk size for testing

            Console.WriteLine("\nTesting business data insertion...");

            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "business");
                stopwatch.Stop();

                Console.WriteLine($"Successfully inserted business data in {stopwatch.ElapsedMilliseconds}ms");

                // Verify the data was inserted
                var count = await _postgresHelper.ExecuteQueryAsync("SELECT COUNT(*) as count FROM business");
                Console.WriteLine($"Total businesses in database: {count[0]["count"]}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to insert business data: {ex.Message}");
            }
        }

        private async Task TestReviewInsertionAsync()
        {
            string filePath = $"{_yelpDatasetPath}/review_reduced.json";
            int chunkSize = 100; // Smaller chunk size for testing

            Console.WriteLine("\nTesting review data insertion...");

            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "review");
                stopwatch.Stop();

                Console.WriteLine($"Successfully inserted review data in {stopwatch.ElapsedMilliseconds}ms");

                // Verify the data was inserted
                var count = await _postgresHelper.ExecuteQueryAsync("SELECT COUNT(*) as count FROM review");
                Console.WriteLine($"Total reviews in database: {count[0]["count"]}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to insert review data: {ex.Message}");
            }
        }

        private async Task TestUserInsertionAsync()
        {
            string filePath = $"{_yelpDatasetPath}/user_reduced.json";
            int chunkSize = 100; // Smaller chunk size for testing

            Console.WriteLine("\nTesting user data insertion...");

            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "users");
                stopwatch.Stop();

                Console.WriteLine($"Successfully inserted user data in {stopwatch.ElapsedMilliseconds}ms");

                // Verify the data was inserted
                var count = await _postgresHelper.ExecuteQueryAsync("SELECT COUNT(*) as count FROM user");
                Console.WriteLine($"Total users in database: {count[0]["count"]}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to insert user data: {ex.Message}");
            }
        }

        private async Task TestTipInsertionAsync()
        {
            string filePath = $"{_yelpDatasetPath}/tip_reduced.json";
            int chunkSize = 100; // Smaller chunk size for testing

            Console.WriteLine("\nTesting tip data insertion...");

            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "tip");
                stopwatch.Stop();

                Console.WriteLine($"Successfully inserted tip data in {stopwatch.ElapsedMilliseconds}ms");

                // Verify the data was inserted
                var count = await _postgresHelper.ExecuteQueryAsync("SELECT COUNT(*) as count FROM tip");
                Console.WriteLine($"Total tips in database: {count[0]["count"]}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to insert tip data: {ex.Message}");
            }
        }

        private async Task TestCheckinInsertionAsync()
        {
            string filePath = $"{_yelpDatasetPath}/checkin_reduced.json";
            int chunkSize = 100; // Smaller chunk size for testing

            Console.WriteLine("\nTesting checkin data insertion...");

            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, chunkSize, "checkin");
                stopwatch.Stop();

                Console.WriteLine($"Successfully inserted checkin data in {stopwatch.ElapsedMilliseconds}ms");

                // Verify the data was inserted
                var count = await _postgresHelper.ExecuteQueryAsync("SELECT COUNT(*) as count FROM checkin");
                Console.WriteLine($"Total checkins in database: {count[0]["count"]}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to insert checkin data: {ex.Message}");
            }
        }

        private async Task TestQueryPerformanceAsync()
        {
            Console.WriteLine("\nTesting PostgreSQL query performance...");
            Console.WriteLine("=====================================");

            // Test business queries
            await TestBusinessQueriesAsync();

            // Test review queries
            await TestReviewQueriesAsync();

            // Test user queries
            await TestUserQueriesAsync();

            // Test tip queries
            await TestTipQueriesAsync();

            // Test checkin queries
            await TestCheckinQueriesAsync();
        }

        private async Task TestBusinessQueriesAsync()
        {
            Console.WriteLine("\nTesting business queries...");

            // Query 1: Find businesses by rating
            Console.WriteLine("Query 1: Find businesses by rating (stars > 4.0)");

            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var results = await _postgresHelper.ExecuteQueryAsync(
                    "SELECT * FROM business WHERE stars > 4.0 LIMIT 100");
                stopwatch.Stop();

                Console.WriteLine($"Query executed in {stopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"Found {results.Count} businesses with rating > 4.0");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to execute business rating query: {ex.Message}");
            }

            // Query 2: Find businesses by location
            Console.WriteLine("\nQuery 2: Find businesses by location (within 5km of a point)");

            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var results = await _postgresHelper.ExecuteQueryAsync(
                    "SELECT * FROM business WHERE earth_box(ll_to_earth(37.7749, -122.4194), 5000) @> ll_to_earth(latitude, longitude) LIMIT 100");
                stopwatch.Stop();

                Console.WriteLine($"Query executed in {stopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"Found {results.Count} businesses within 5km of the specified point");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to execute business location query: {ex.Message}");
            }
        }

        private async Task TestReviewQueriesAsync()
        {
            Console.WriteLine("\nTesting review queries...");

            // Query 1: Find reviews by business ID
            Console.WriteLine("Query 1: Find reviews by business ID");

            try
            {
                // Get a sample business ID
                var sampleBusiness = await _postgresHelper.ExecuteQueryAsync("SELECT business_id FROM business LIMIT 1");
                if (sampleBusiness.Count == 0)
                {
                    Console.WriteLine("No businesses found in the database.");
                    return;
                }

                string businessId = sampleBusiness[0]["business_id"].ToString();

                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var results = await _postgresHelper.ExecuteQueryAsync(
                    $"SELECT * FROM review WHERE business_id = '{businessId}' LIMIT 100");
                stopwatch.Stop();

                Console.WriteLine($"Query executed in {stopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"Found {results.Count} reviews for business {businessId}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to execute review query: {ex.Message}");
            }
        }

        private async Task TestUserQueriesAsync()
        {
            Console.WriteLine("\nTesting user queries...");

            // Query 1: Find users by name
            Console.WriteLine("Query 1: Find user by name (contains 'John')");

            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var results = await _postgresHelper.ExecuteQueryAsync(
                    "SELECT * FROM user WHERE name LIKE '%John%' LIMIT 100");
                stopwatch.Stop();

                Console.WriteLine($"Query executed in {stopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"Found {results.Count} user with 'John' in their name");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to execute user query: {ex.Message}");
            }
        }

        private async Task TestTipQueriesAsync()
        {
            Console.WriteLine("\nTesting tip queries...");

            // Query 1: Find tips by business ID
            Console.WriteLine("Query 1: Find tips by business ID");

            try
            {
                // Get a sample business ID
                var sampleBusiness = await _postgresHelper.ExecuteQueryAsync("SELECT business_id FROM business LIMIT 1");
                if (sampleBusiness.Count == 0)
                {
                    Console.WriteLine("No businesses found in the database.");
                    return;
                }

                string businessId = sampleBusiness[0]["business_id"].ToString();

                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var results = await _postgresHelper.ExecuteQueryAsync(
                    $"SELECT * FROM tip WHERE business_id = '{businessId}' LIMIT 100");
                stopwatch.Stop();

                Console.WriteLine($"Query executed in {stopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"Found {results.Count} tips for business {businessId}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to execute tip query: {ex.Message}");
            }
        }

        private async Task TestCheckinQueriesAsync()
        {
            Console.WriteLine("\nTesting checkin queries...");

            // Query 1: Find checkins by business ID
            Console.WriteLine("Query 1: Find checkins by business ID");

            try
            {
                // Get a sample business ID
                var sampleBusiness = await _postgresHelper.ExecuteQueryAsync("SELECT business_id FROM business LIMIT 1");
                if (sampleBusiness.Count == 0)
                {
                    Console.WriteLine("No businesses found in the database.");
                    return;
                }

                string businessId = sampleBusiness[0]["business_id"].ToString();

                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var results = await _postgresHelper.ExecuteQueryAsync(
                    $"SELECT * FROM checkin WHERE business_id = '{businessId}' LIMIT 100");
                stopwatch.Stop();

                Console.WriteLine($"Query executed in {stopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"Found {results.Count} checkins for business {businessId}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to execute checkin query: {ex.Message}");
            }
        }
    }
}