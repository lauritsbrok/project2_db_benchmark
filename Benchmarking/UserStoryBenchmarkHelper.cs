namespace project2_db_benchmark.Benchmarking;

public class UserStoryBenchmarkHelper
{
    private readonly string _instructionSetFile;
    private readonly InstructionExecutor _instructionExecutor;

    public UserStoryBenchmarkHelper(string instructionSetFile = "instruction-set.json", int numInstructions = 100000)
    {
        _instructionSetFile = instructionSetFile;
        
        // Generate a new instruction file for each run
        var gen = new Generator.Generator(123);
        gen.Generate(numInstructions, _instructionSetFile).GetAwaiter().GetResult();
        
        _instructionExecutor = new InstructionExecutor(_instructionSetFile);
    }

    public async Task<(double, double, List<double>)> RunBenchmarkAsyncMongo(int num_concurrent)
    {
        (double mongo_instr_totalTime, double mongo_instr_throughput, var mongo_instr_latencies) = 
            await _instructionExecutor.BenchmarkMongoDbAsync(num_concurrent);

        return (mongo_instr_totalTime, mongo_instr_throughput, mongo_instr_latencies);
    }

    public async Task<(double, double, List<double>)> RunBenchmarkAsyncPostgres(int num_concurrent)
    {
        (double postgres_instr_totalTime, double postgres_instr_throughput, var postgres_instr_latencies) = 
            await _instructionExecutor.BenchmarkPostgresAsync(num_concurrent);
            
        return (postgres_instr_totalTime, postgres_instr_throughput, postgres_instr_latencies);
    }
}