using System.Text.Json;

namespace project2_db_benchmark.Parser;

public static class Parser
{
    public static async Task<IEnumerable<T>> Parse<T>(string filePath)
    {
        var records = new List<T>();

        await foreach (var line in File.ReadLinesAsync(filePath))
        {
            if (string.IsNullOrWhiteSpace(line)) continue;

            try
            {
                if (JsonSerializer.Deserialize<T>(line) is T item)
                    records.Add(item);
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"Error parsing line: {ex.Message}");
            }
        }

        return records;
    }
}