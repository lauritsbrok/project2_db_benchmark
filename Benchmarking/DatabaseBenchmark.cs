using System.Diagnostics;
using project2_db_benchmark.mongodb;
using project2_db_benchmark.mongodb.Models;
// assuming this namespace for postgres helper
using project2_db_benchmark.postgres;

namespace project2_db_benchmark.Benchmarking
{
    public class DatabaseBenchmark
    {
        private readonly MongoDatabaseHelper _mongoHelper;
        private readonly PostgresDatabaseHelper _postgresHelper;
        private readonly Stopwatch _stopwatch = new();
        
        public DatabaseBenchmark(MongoDatabaseHelper mongoHelper, PostgresDatabaseHelper postgresHelper)
        {
            _mongoHelper = mongoHelper;
            _postgresHelper = postgresHelper;
        }
        
        // Benchmark data import
        public async Task<BenchmarkResult> BenchmarkImportAsync(string filePath, int batchSize, ImportType type)
        {
            _stopwatch.Reset();
            _stopwatch.Start();
            
            try
            {
                switch (type)
                {
                    case ImportType.MongoBusinesses:
                        await _mongoHelper.ImportBusinessesFromFileAsync(filePath, batchSize);
                        break;
                    case ImportType.MongoReviews:
                        await _mongoHelper.ImportReviewsFromFileAsync(filePath, batchSize);
                        break;
                    case ImportType.MongoUsers:
                        await _mongoHelper.ImportUsersFromFileAsync(filePath, batchSize);
                        break;
                    case ImportType.MongoTips:
                        await _mongoHelper.ImportTipsFromFileAsync(filePath, batchSize);
                        break;
                    case ImportType.MongoCheckins:
                        await _mongoHelper.ImportCheckinsFromFileAsync(filePath, batchSize);
                        break;
                    case ImportType.MongoPhotos:
                        await _mongoHelper.ImportPhotosFromFileAsync(filePath, batchSize);
                        break;
                    case ImportType.PostgresBusinesses:
                        // Using method from PostgresDatabaseHelper
                        await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, batchSize, "businesses");
                        break;
                    case ImportType.PostgresReviews:
                        await _postgresHelper.InsertJsonFromFileInChunksAsync(filePath, batchSize, "reviews");
                        break;
                    // Add other PostgreSQL import types as needed
                    default:
                        throw new ArgumentException("Invalid import type", nameof(type));
                }
                
                _stopwatch.Stop();
                
                return new BenchmarkResult
                {
                    OperationType = type.ToString(),
                    DurationMs = _stopwatch.ElapsedMilliseconds,
                    Success = true
                };
            }
            catch (Exception ex)
            {
                _stopwatch.Stop();
                
                return new BenchmarkResult
                {
                    OperationType = type.ToString(),
                    DurationMs = _stopwatch.ElapsedMilliseconds,
                    Success = false,
                    ErrorMessage = ex.Message
                };
            }
        }
        
        // Benchmark specific queries
        public async Task<BenchmarkResult> BenchmarkQueryAsync(QueryType type, object parameters)
        {
            _stopwatch.Reset();
            _stopwatch.Start();
            
            try
            {
                // Implement various query types
                switch (type)
                {
                    case QueryType.MongoBusinessByRating:
                        // Example: Find businesses with rating greater than X
                        var minRating = (double)parameters;
                        var businesses = await _mongoHelper.FindBusinessesByRatingAsync(minRating, 100);
                        break;
                        
                    case QueryType.MongoReviewsByBusiness:
                        // Example: Find reviews for a business
                        var businessId = (string)parameters;
                        var reviews = await _mongoHelper.FindReviewsByBusinessIdAsync(businessId, 100);
                        break;
                        
                    case QueryType.MongoUsersByName:
                        // Example: Find users matching a name pattern
                        var namePattern = (string)parameters;
                        var users = await _mongoHelper.FindUsersByNameAsync(namePattern, 100);
                        break;
                        
                    case QueryType.MongoBusinessesByLocation:
                        // Example: Find businesses near a location
                        var locationParams = (LocationParams)parameters;
                        var locationBusinesses = await _mongoHelper.FindBusinessesByLocationAsync(
                            locationParams.Longitude, 
                            locationParams.Latitude, 
                            locationParams.MaxDistanceMeters, 
                            100);
                        break;
                        
                    case QueryType.PostgresBusinessByRating:
                        // Example query for Postgres
                        var pgMinRating = (double)parameters;
                        var pgResults = await _postgresHelper.ExecuteQueryAsync($"SELECT * FROM businesses WHERE stars > {pgMinRating} LIMIT 100");
                        break;
                        
                    // Add more query types as needed
                    
                    default:
                        throw new ArgumentException("Invalid query type", nameof(type));
                }
                
                _stopwatch.Stop();
                
                return new BenchmarkResult
                {
                    OperationType = type.ToString(),
                    DurationMs = _stopwatch.ElapsedMilliseconds,
                    Success = true
                };
            }
            catch (Exception ex)
            {
                _stopwatch.Stop();
                
                return new BenchmarkResult
                {
                    OperationType = type.ToString(),
                    DurationMs = _stopwatch.ElapsedMilliseconds,
                    Success = false,
                    ErrorMessage = ex.Message
                };
            }
        }
        
        // Run a series of benchmarks and compare results
        public async Task<List<BenchmarkResult>> RunComparisonAsync()
        {
            var results = new List<BenchmarkResult>();
            
            // Import benchmarks
            // results.Add(await BenchmarkImportAsync("yelp_dataset/yelp_academic_dataset_business.json", 1000, ImportType.MongoBusinesses));
            // results.Add(await BenchmarkImportAsync("yelp_dataset/yelp_academic_dataset_business.json", 1000, ImportType.PostgresBusinesses));
            
            // Query benchmarks
            results.Add(await BenchmarkQueryAsync(QueryType.MongoBusinessByRating, 4.0));
            results.Add(await BenchmarkQueryAsync(QueryType.PostgresBusinessByRating, 4.0));
            
            // Location search benchmark
            results.Add(await BenchmarkQueryAsync(QueryType.MongoBusinessesByLocation, 
                new LocationParams
                {
                    Longitude = -122.3961,
                    Latitude = 37.78175,
                    MaxDistanceMeters = 5000
                }));
            
            return results;
        }
    }
    
    public class LocationParams
    {
        public double Longitude { get; set; }
        public double Latitude { get; set; }
        public double MaxDistanceMeters { get; set; }
    }
    
    public enum ImportType
    {
        MongoBusinesses,
        MongoReviews,
        MongoUsers,
        MongoTips,
        MongoCheckins,
        MongoPhotos,
        PostgresBusinesses,
        PostgresReviews,
        PostgresUsers,
        PostgresTips,
        PostgresCheckins,
        PostgresPhotos
    }
    
    public enum QueryType
    {
        MongoBusinessByRating,
        MongoReviewsByBusiness,
        MongoUsersByName,
        MongoBusinessesByLocation,
        PostgresBusinessByRating,
        PostgresReviewsByBusiness,
        PostgresUsersByName
    }
    
    public class BenchmarkResult
    {
        public string OperationType { get; set; }
        public long DurationMs { get; set; }
        public bool Success { get; set; }
        public string ErrorMessage { get; set; }
        
        public override string ToString()
        {
            return $"{OperationType}: {DurationMs}ms, Success: {Success}{(Success ? "" : $", Error: {ErrorMessage}")}";
        }
    }
} 