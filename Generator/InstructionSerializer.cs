using System.Text.Json;

namespace project2_db_benchmark.Generator;

public static class InstructionSerializer
{
    public static async Task SaveInstructionsAsync(List<Instruction> instructions, string filePath)
    {
        var options = new JsonSerializerOptions
        {
            WriteIndented = true
        };

        using FileStream fs = File.Create(filePath);
        await JsonSerializer.SerializeAsync(fs, instructions, options);
    }

    public static async Task<List<Instruction>> LoadInstructionsAsync(string filePath)
    {
        using FileStream fs = File.OpenRead(filePath);
        return await JsonSerializer.DeserializeAsync<List<Instruction>>(fs) ?? new List<Instruction>();
    }
}
