using System.Diagnostics;
using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Generator;
using project2_db_benchmark.Models;
using project2_db_benchmark.Helpers;
using MongoDB.Driver;

namespace project2_db_benchmark.Benchmarking;

/// <summary>
/// Executes a set of instructions against both MongoDB and Postgres databases
/// to benchmark their performance with realistic user behavior patterns.
/// </summary>
public class InstructionExecutor
{
    private readonly MongoDatabaseHelper _mongoHelper;
    private readonly PostgresDatabaseHelper _postgresHelper;
    private readonly List<Instruction> _instructions;

    public InstructionExecutor(string instructionSetPath)
    {
        _mongoHelper = new MongoDatabaseHelper();
        _postgresHelper = new PostgresDatabaseHelper();
        _instructions = InstructionSerializer.LoadInstructionsAsync(instructionSetPath).Result;

        Console.WriteLine($"Loaded {_instructions.Count} instructions from {instructionSetPath}");
    }

    /// <summary>
    /// Benchmarks the execution of instructions against MongoDB
    /// </summary>
    public async Task<(double TotalTime, double Throughput, List<double> Latencies)> BenchmarkMongoDbAsync(int concurrencyLevel = 8)
    {

        var instructionExecutions = new List<Func<Task>>();
        instructionExecutions.AddRange(_instructions.Select<Instruction, Func<Task>>(b => () => ExecuteMongoInstruction(b)));
        var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(instructionExecutions, concurrencyLevel);

        double throughput = _instructions.Count / totalTime;

        return (totalTime, throughput, latencies);
    }

    /// <summary>
    /// Benchmarks the execution of instructions against PostgreSQL
    /// </summary>
    public async Task<(double TotalTime, double Throughput, List<double> Latencies)> BenchmarkPostgresAsync(int concurrencyLevel = 8)
    {
        var instructionExecutions = new List<Func<Task>>();
        instructionExecutions.AddRange(_instructions.Select<Instruction, Func<Task>>(b => () => ExecutePostgresInstruction(b)));
        var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(instructionExecutions, concurrencyLevel);

        double throughput = _instructions.Count / totalTime;

        return (totalTime, throughput, latencies);
    }

    /// <summary>
    /// Execute a single instruction against MongoDB
    /// </summary>
    private async Task ExecuteMongoInstruction(Instruction instruction)
    {
        switch (instruction.Type)
        {
            case InstructionType.SearchForBusinesses:
                // 1. Search for businesses by name prefix
                string namePrefix = instruction.Parameters["name_prefix"];
                var matchedBusinesses = await _mongoHelper.SearchBusinessesByNamePrefixAsync(namePrefix, 10);
                
                if (matchedBusinesses.Count > 0)
                {
                    // 2. For the first 3 matched businesses (or fewer if less than 3 returned)
                    for (int i = 0; i < Math.Min(3, matchedBusinesses.Count); i++)
                    {
                        var business = matchedBusinesses[i];
                        
                        // 3. Fetch images for the business
                        var businessPhotos = await _mongoHelper.GetPhotosByBusinessIdAsync(business.BusinessId);
                        
                        // 4. Fetch 10 most recent reviews
                        var recentReviews = await _mongoHelper.GetMostRecentReviewsByBusinessIdAsync(business.BusinessId, 10);
                        
                        // 5. Get users who wrote those reviews
                        var userIds = recentReviews.Select(r => r.UserId).Distinct().ToList();
                        var reviewUsers = await _mongoHelper.GetUsersByReviewIdsAsync(userIds);
                        
                        // 6. Sort reviews by proximity to 3-star ratings
                        var sortedReviews = await _mongoHelper.GetReviewsByBusinessIdSortedByStarsAsync(business.BusinessId, 3.0);
                    }
                }
                break;

            case InstructionType.SubmitReviews:
                // 1. Create a new user
                var submitUser = new User
                {
                    UserId = Guid.NewGuid().ToString(),
                    Name = instruction.Parameters["user_name"],
                    ReviewCount = 0,
                    YelpingSince = DateTime.UtcNow.ToString("yyyy-MM-dd")
                };
                await _mongoHelper.InsertUserAsync(submitUser);
                // 2. Search for businesses by name prefix
                string submitNamePrefix = instruction.Parameters["name_prefix"];
                var submitBusinesses = await _mongoHelper.SearchBusinessesByNamePrefixAsync(submitNamePrefix, 10);
                
                if (submitBusinesses.Count > 0)
                {
                    // 3. Select the first matching business
                    var targetBusiness = submitBusinesses[0];
                    
                    // 4. Fetch its reviews
                    var businessReviews = await _mongoHelper.GetReviewsByBusinessIdAsync(targetBusiness.BusinessId);
                    
                    // 5. Retrieve the users who wrote those reviews
                    var reviewerIds = businessReviews.Select(r => r.UserId).Distinct().ToList();
                    var reviewers = await _mongoHelper.GetUsersByReviewIdsAsync(reviewerIds);
                    
                    // 6. Submit a new review for the business
                    var submitReview = new Review
                    {
                        ReviewId = Guid.NewGuid().ToString(),
                        UserId = submitUser.UserId,
                        BusinessId = targetBusiness.BusinessId,
                        Stars = double.Parse(instruction.Parameters["stars"]),
                        Text = instruction.Parameters["text"],
                        Date = DateTime.UtcNow.ToString("yyyy-MM-dd")
                    };
                    await _mongoHelper.InsertReviewAsync(submitReview);
                }
                break;

            default:
                throw new NotImplementedException($"Instruction type {instruction.Type} not implemented");
        }
    }

    /// <summary>
    /// Execute a single instruction against PostgreSQL
    /// </summary>
    private async Task ExecutePostgresInstruction(Instruction instruction)
    {
        switch (instruction.Type)
        {
            case InstructionType.SearchForBusinesses:
                // 1. Search for businesses by name prefix
                string namePrefix = instruction.Parameters["name_prefix"];
                var matchedBusinesses = await _postgresHelper.SearchBusinessesByNamePrefixAsync(namePrefix, 10);
                
                if (matchedBusinesses.Count > 0)
                {
                    // 2. For the first 3 matched businesses (or fewer if less than 3 returned)
                    for (int i = 0; i < Math.Min(3, matchedBusinesses.Count); i++)
                    {
                        var business = matchedBusinesses[i];
                        
                        // 3. Fetch images for the business
                        var businessPhotos = await _postgresHelper.GetPhotosByBusinessIdAsync(business.BusinessId);
                        
                        // 4. Fetch 10 most recent reviews
                        var recentReviews = await _postgresHelper.GetMostRecentReviewsByBusinessIdAsync(business.BusinessId, 10);
                        
                        // 5. Get users who wrote those reviews
                        var userIds = recentReviews.Select(r => r.UserId).Distinct().ToList();
                        var reviewUsers = await _postgresHelper.GetUsersByReviewIdsAsync(userIds);
                        
                        // 6. Sort reviews by proximity to 3-star ratings
                        var sortedReviews = await _postgresHelper.GetReviewsByBusinessIdSortedByStarsAsync(business.BusinessId, 3.0);
                    }
                }
                break;

            case InstructionType.SubmitReviews:
                // 1. Create a new user
                var submitUser = new User
                {
                    UserId = Guid.NewGuid().ToString(),
                    Name = instruction.Parameters["user_name"],
                    ReviewCount = 0,
                    YelpingSince = DateTime.UtcNow.ToString("yyyy-MM-dd")
                };
                await _postgresHelper.InsertUserAsync(submitUser);
                
                // 2. Search for businesses by name prefix
                string submitNamePrefix = instruction.Parameters["name_prefix"];
                var submitBusinesses = await _postgresHelper.SearchBusinessesByNamePrefixAsync(submitNamePrefix, 10);
                
                if (submitBusinesses.Count > 0)
                {
                    // 3. Select the first matching business
                    var targetBusiness = submitBusinesses[0];
                    
                    // 4. Fetch its reviews
                    var businessReviews = await _postgresHelper.GetReviewsByBusinessIdAsync(targetBusiness.BusinessId);
                    
                    // 5. Retrieve the users who wrote those reviews
                    var reviewerIds = businessReviews.Select(r => r.UserId).Distinct().ToList();
                    var reviewers = await _postgresHelper.GetUsersByReviewIdsAsync(reviewerIds);
                    
                    // 6. Submit a new review for the business
                    var submitReview = new Review
                    {
                        ReviewId = Guid.NewGuid().ToString(),
                        UserId = submitUser.UserId,
                        BusinessId = targetBusiness.BusinessId,
                        Stars = double.Parse(instruction.Parameters["stars"]),
                        Text = instruction.Parameters["text"],
                        Date = DateTime.UtcNow.ToString("yyyy-MM-dd")
                    };
                    await _postgresHelper.InsertReviewAsync(submitReview);
                }
                break;

            default:
                throw new NotImplementedException($"Instruction type {instruction.Type} not implemented");
        }
    }
}