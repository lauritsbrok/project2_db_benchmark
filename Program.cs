using System.Diagnostics;
using project2_db_benchmark.Benchmarking;
using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Models.MongoDB;
using project2_db_benchmark.Models.Shared;
using project2_db_benchmark.Parser;

// Initialize MongoDB helper
var mongoHelper = new MongoDatabaseHelper();
var postgresHelper = new PostgresDatabaseHelper();
var databaseImportHelper = new DatabaseImportHelper(mongoHelper, postgresHelper);

// Example MongoDB usage
await DemoMongoOperationsAsync(mongoHelper);

Console.WriteLine("Demo completed");

// Demo MongoDB operations
async Task DemoMongoOperationsAsync(MongoDatabaseHelper dbHelper)
{
    Console.WriteLine("Starting MongoDB demo...");
    

    // Timing insert operations
    var stopwatch = Stopwatch.StartNew();
    
    try
    {
        Console.WriteLine("Inserting sample business...");
        IEnumerable<Business> businesses = await Parser.Parse<Business>("yelp_dataset/yelp_academic_dataset_business.json");
        IEnumerable<Photo> photos = await Parser.Parse<Photo>("yelp_dataset/yelp_academic_dataset_photo.json");
        businesses = Business.AttachPhotosToBusinesses(businesses, photos);
        var t = businesses.Where(s => s.Photos.Count() > 0).Count();
        await databaseImportHelper.ImportAsync(businesses);
        Console.WriteLine($"Business inserted in {stopwatch.ElapsedMilliseconds}ms");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error inserting business: {ex.Message}");
    }
}