using System.Text.Json;

namespace project2_db_benchmark.Generator;

public static class SampleLoader
{
    public static async Task<List<T>> LoadFromJsonFile<T>(string path)
    {
        var json = await File.ReadAllTextAsync(path);
        return JsonSerializer.Deserialize<List<T>>(json) ?? [];
    }
}
