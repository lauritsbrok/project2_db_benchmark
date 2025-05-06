namespace project2_db_benchmark.Benchmarking;

public class UserStoryBenchmarkHelper(string instructionSetFile = "instruction-set.json", int numInstructions = 50000)
{
    public async Task RunBenchmarkAsync()
    {
        // Delete existing file if it exists
        if (File.Exists(instructionSetFile))
        {
            File.Delete(instructionSetFile);
        }

        Console.WriteLine($"\nGenerating instruction set...");
        var gen = new Generator.Generator(123);
        await gen.Generate(numInstructions, instructionSetFile);

        // Benchmark realistic user behavior using instructions
        Console.WriteLine("\nBenchmarking realistic user behavior with instructions...");
        var instructionExecutor = new InstructionExecutor(instructionSetFile);

        Console.WriteLine("\nExecuting instructions against MongoDB...");
        (double mongo_instr_totalTime, double mongo_instr_throughput, var mongo_instr_latencies) = await instructionExecutor.BenchmarkMongoDbAsync();
        Console.WriteLine($"MongoDB instruction benchmark took {mongo_instr_totalTime} seconds");
        Console.WriteLine($"Average throughput: {mongo_instr_throughput:F2} instructions/second");
        Console.WriteLine($"Average latency: {mongo_instr_latencies.Average():F2} ms");

        Console.WriteLine("\nExecuting instructions against PostgreSQL...");
        (double postgres_instr_totalTime, double postgres_instr_throughput, var postgres_instr_latencies) = await instructionExecutor.BenchmarkPostgresAsync();
        Console.WriteLine($"PostgreSQL instruction benchmark took {postgres_instr_totalTime} seconds");
        Console.WriteLine($"Average throughput: {postgres_instr_throughput:F2} instructions/second");
        Console.WriteLine($"Average latency: {postgres_instr_latencies.Average():F2} ms");

        // Compare the results
        Console.WriteLine("\n--- Performance Comparison ---");
        Console.WriteLine($"MongoDB average instruction latency: {mongo_instr_latencies.Average():F2} ms");
        Console.WriteLine($"PostgreSQL average instruction latency: {postgres_instr_latencies.Average():F2} ms");
        Console.WriteLine($"Difference: {Math.Abs(mongo_instr_latencies.Average() - postgres_instr_latencies.Average()):F2} ms");
        Console.WriteLine($"MongoDB is {(mongo_instr_throughput > postgres_instr_throughput ? "faster" : "slower")} than PostgreSQL for this workload");
        Console.WriteLine($"Performance ratio: {mongo_instr_throughput / postgres_instr_throughput:F2}x");
    }
}