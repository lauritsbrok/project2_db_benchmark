using System.Diagnostics;
using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Generator;
using project2_db_benchmark.Models.Shared;
using project2_db_benchmark.Models.MongoDB;
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
            case InstructionType.CreateUser:
                var newUser = new User
                {
                    UserId = Guid.NewGuid().ToString(),
                    Name = instruction.Parameters["name"],
                    ReviewCount = 0,
                    YelpingSince = DateTime.UtcNow.ToString("yyyy-MM-dd")
                };
                await _mongoHelper.InsertUserAsync(newUser);
                break;

            case InstructionType.SearchBusiness:
                string category = instruction.Parameters["category"];
                string city = instruction.Parameters["city"];
                var businesses = await _mongoHelper.SearchBusinessesByCategoryAndCityAsync(category, city);
                break;

            case InstructionType.ViewBusiness:
                string businessId = instruction.Parameters["business_id"];
                await _mongoHelper.GetBusinessByIdAsync(businessId);
                break;

            case InstructionType.PostReview:
                var newReview = new Review
                {
                    ReviewId = Guid.NewGuid().ToString(),
                    UserId = instruction.Parameters["user_id"],
                    BusinessId = instruction.Parameters["business_id"],
                    Stars = double.Parse(instruction.Parameters["stars"]),
                    Text = instruction.Parameters["text"],
                    Date = DateTime.UtcNow.ToString("yyyy-MM-dd")
                };
                await _mongoHelper.InsertReviewAsync(newReview);
                break;

            case InstructionType.PostTip:
                var newTip = new Tip
                {
                    UserId = instruction.Parameters["user_id"],
                    BusinessId = instruction.Parameters["business_id"],
                    Text = instruction.Parameters["text"],
                    Date = DateTime.UtcNow.ToString("yyyy-MM-dd"),
                    ComplimentCount = 0
                };
                await _mongoHelper.InsertTipAsync(newTip);
                break;

            case InstructionType.ViewUser:
                string userId = instruction.Parameters["user_id"];
                await _mongoHelper.GetUserByIdAsync(userId);
                break;

            case InstructionType.ViewPhotos:
                string photoBusinessId = instruction.Parameters["business_id"];
                var photos = await _mongoHelper.GetPhotosByBusinessIdAsync(photoBusinessId);
                break;

            case InstructionType.Checkin:
                var newCheckin = new Checkin
                {
                    BusinessId = instruction.Parameters["business_id"],
                    Date = DateTime.Parse(instruction.Parameters["timestamp"]).ToString("yyyy-MM-dd")
                };
                await _mongoHelper.InsertCheckinAsync(newCheckin);
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
            case InstructionType.CreateUser:
                var newUser = new User
                {
                    UserId = Guid.NewGuid().ToString(),
                    Name = instruction.Parameters["name"],
                    ReviewCount = 0,
                    YelpingSince = DateTime.UtcNow.ToString("yyyy-MM-dd")
                };
                await _postgresHelper.InsertUserAsync(newUser);
                break;

            case InstructionType.SearchBusiness:
                string category = instruction.Parameters["category"];
                string city = instruction.Parameters["city"];
                var businesses = await _postgresHelper.SearchBusinessesByCategoryAndCityAsync(category, city);
                break;

            case InstructionType.ViewBusiness:
                string businessId = instruction.Parameters["business_id"];
                await _postgresHelper.GetBusinessByIdAsync(businessId);
                break;

            case InstructionType.PostReview:
                var newReview = new Review
                {
                    ReviewId = Guid.NewGuid().ToString(),
                    UserId = instruction.Parameters["user_id"],
                    BusinessId = instruction.Parameters["business_id"],
                    Stars = double.Parse(instruction.Parameters["stars"]),
                    Text = instruction.Parameters["text"],
                    Date = DateTime.UtcNow.ToString("yyyy-MM-dd")
                };
                await _postgresHelper.InsertReviewAsync(newReview);
                break;

            case InstructionType.PostTip:
                var newTip = new Tip
                {
                    UserId = instruction.Parameters["user_id"],
                    BusinessId = instruction.Parameters["business_id"],
                    Text = instruction.Parameters["text"],
                    Date = DateTime.UtcNow.ToString("yyyy-MM-dd"),
                    ComplimentCount = 0
                };
                await _postgresHelper.InsertTipAsync(newTip);
                break;

            case InstructionType.ViewUser:
                string userId = instruction.Parameters["user_id"];
                await _postgresHelper.GetUserByIdAsync(userId);
                break;

            case InstructionType.ViewPhotos:
                string photoBusinessId = instruction.Parameters["business_id"];
                var photos = await _postgresHelper.GetPhotosByBusinessIdAsync(photoBusinessId);
                break;

            case InstructionType.Checkin:
                var newCheckin = new Checkin
                {
                    BusinessId = instruction.Parameters["business_id"],
                    Date = DateTime.Parse(instruction.Parameters["timestamp"]).ToString("yyyy-MM-dd")
                };
                await _postgresHelper.InsertCheckinAsync(newCheckin);
                break;

            default:
                throw new NotImplementedException($"Instruction type {instruction.Type} not implemented");
        }
    }
}