namespace project2_db_benchmark.Benchmarking;

public class UserStoryBenchmarkHelper(string instructionSetFile = "instruction-set.json", int numInstructions = 50000)
{
    public async Task<(double, double, List<double>)> RunBenchmarkAsyncMongo(int num_concurrent)
    {
        // Delete existing file if it exists
        if (!File.Exists(instructionSetFile))
        {
            var gen = new Generator.Generator(123);
            await gen.Generate(numInstructions, instructionSetFile);
        }

        var instructionExecutor = new InstructionExecutor(instructionSetFile);

        (double mongo_instr_totalTime, double mongo_instr_throughput, var mongo_instr_latencies) = await instructionExecutor.BenchmarkMongoDbAsync(num_concurrent);

        return (mongo_instr_totalTime, mongo_instr_throughput, mongo_instr_latencies);

    }

    public async Task<(double, double, List<double>)> RunBenchmarkAsyncPostgres(int num_concurrent)
    {
        // Delete existing file if it exists
        if (File.Exists(instructionSetFile))
        {
            File.Delete(instructionSetFile);
        }

        var gen = new Generator.Generator(123);
        await gen.Generate(numInstructions, instructionSetFile);

        var instructionExecutor = new InstructionExecutor(instructionSetFile);

        (double postgres_instr_totalTime, double postgres_instr_throughput, var postgres_instr_latencies) = await instructionExecutor.BenchmarkPostgresAsync(num_concurrent);
        return (postgres_instr_totalTime, postgres_instr_throughput, postgres_instr_latencies);
    }

}