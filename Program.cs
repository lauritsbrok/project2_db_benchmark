using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GeoJsonObjectModel;
using DotNetEnv;
using project2_db_benchmark.mongodb;
using project2_db_benchmark.mongodb.Models;
using System.Diagnostics;
// Uncomment when ready to use benchmarking
// using project2_db_benchmark.Benchmarking;
// using project2_db_benchmark.postgres;

// Load environment variables
Env.Load();

// MongoDB Connection
var mongouser = Environment.GetEnvironmentVariable("MONGO_INITDB_ROOT_USERNAME");
var mongopass = Environment.GetEnvironmentVariable("MONGO_INITDB_ROOT_PASSWORD");
var mongoport = 27017;
var mongoConnectionString = $"mongodb://{mongouser}:{mongopass}@localhost:{mongoport}";

// Initialize MongoDB helper
var mongoHelper = new MongoDatabaseHelper(mongoConnectionString);

// Example MongoDB usage
await DemoMongoOperationsAsync(mongoHelper);

// Uncomment to run benchmarks
// await RunBenchmarksAsync(mongoHelper);

Console.WriteLine("Demo completed");

// Demo MongoDB operations
async Task DemoMongoOperationsAsync(MongoDatabaseHelper dbHelper)
{
    Console.WriteLine("Starting MongoDB demo...");
    
    // Create a sample business with proper GeoJSON formatting
    var business = new Business
    {
        Id = ObjectId.GenerateNewId().ToString(),
        Name = "Sample Restaurant",
        Address = new Address
        {
            Street = "123 Main St",
            City = "San Francisco",
            State = "CA",
            PostalCode = "94105"
        },
        Location = new GeoJsonPoint<GeoJson2DGeographicCoordinates>(
            new GeoJson2DGeographicCoordinates(-122.3961, 37.78175)
        ),
        Categories = new List<string> { "American", "Burgers" },
        Hours = new Dictionary<string, string>
        {
            { "Monday", "10:00-21:00" },
            { "Tuesday", "10:00-21:00" },
            { "Wednesday", "10:00-21:00" },
            { "Thursday", "10:00-21:00" },
            { "Friday", "10:00-22:00" },
            { "Saturday", "10:00-22:00" },
            { "Sunday", "11:00-20:00" }
        },
        IsOpen = true,
        RatingAvg = 4.5,
        RatingCount = 0,
        CheckinCount = 0,
        PhotoCount = 0,
        TipCount = 0,
        RecentReviews = new List<RecentReview>(),
        HeroPhotos = new List<HeroPhoto>(),
        CreatedAt = DateTime.UtcNow,
        UpdatedAt = DateTime.UtcNow
    };

    // Timing insert operations
    var stopwatch = Stopwatch.StartNew();
    
    try
    {
        Console.WriteLine("Inserting sample business...");
        await dbHelper.InsertBusinessAsync(business);
        Console.WriteLine($"Business inserted in {stopwatch.ElapsedMilliseconds}ms");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error inserting business: {ex.Message}");
    }
    
    // Uncomment to import data from files
    // Console.WriteLine("Importing data from JSON files...");
    // await dbHelper.ImportBusinessesFromFileAsync("yelp_dataset/yelp_academic_dataset_business.json");
    // await dbHelper.ImportReviewsFromFileAsync("yelp_dataset/yelp_academic_dataset_review.json");
    // await dbHelper.ImportUsersFromFileAsync("yelp_dataset/yelp_academic_dataset_user.json");
    // await dbHelper.ImportTipsFromFileAsync("yelp_dataset/yelp_academic_dataset_tip.json");
    // await dbHelper.ImportCheckinsFromFileAsync("yelp_dataset/yelp_academic_dataset_checkin.json");
}

// Run benchmarks for MongoDB and PostgreSQL
async Task RunBenchmarksAsync(MongoDatabaseHelper mongoHelper)
{
    Console.WriteLine("Starting database benchmarks...");
    
    try
    {
        // Test a basic MongoDB query directly
        var stopwatch = Stopwatch.StartNew();
        var businesses = await mongoHelper.FindBusinessesByRatingAsync(4.0, 5);
        Console.WriteLine($"Found {businesses.Count} businesses with rating > 4.0 in {stopwatch.ElapsedMilliseconds}ms");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error running MongoDB benchmark: {ex.Message}");
    }
    
    // With the PostgresDatabaseHelper, when it's ready
    // try
    // {
    //     using var postgresHelper = new PostgresDatabaseHelper();
    //     var benchmark = new DatabaseBenchmark(mongoHelper, postgresHelper);
    //     var results = await benchmark.RunComparisonAsync();
    //     
    //     foreach (var result in results)
    //     {
    //         Console.WriteLine(result);
    //     }
    // }
    // catch (Exception ex)
    // {
    //     Console.WriteLine($"Error running benchmarks: {ex.Message}");
    // }
}