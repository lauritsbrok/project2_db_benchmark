using project2_db_benchmark.Models.Shared;
using project2_db_benchmark.Models.MongoDB;
using System.Text.Json;

namespace project2_db_benchmark.Generator;

public static class SampleDumper
{
    public static async Task DumpBusinessSample(IEnumerable<Business> businesses, string path, int max = 10000)
    {
        var sample = businesses.Select(b => new
        {
            b.BusinessId,
            b.Name,
            b.City,
            b.State,
            b.Categories
        }).Take(max);

        await WriteToJsonFile(sample, path);
    }

    public static async Task DumpUserSample(IEnumerable<User> users, string path, int max = 10000)
    {
        var sample = users.Select(u => new
        {
            u.UserId,
            u.Name,
            u.ReviewCount
        }).Take(max);

        await WriteToJsonFile(sample, path);
    }

    public static async Task DumpReviewSample(IEnumerable<Review> reviews, string path, int max = 10000)
    {
        var sample = reviews.Select(r => new
        {
            r.ReviewId,
            r.UserId,
            r.BusinessId,
            r.Stars
        }).Take(max);

        await WriteToJsonFile(sample, path);
    }

    private static async Task WriteToJsonFile<T>(IEnumerable<T> data, string path)
    {
        var json = JsonSerializer.Serialize(data, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(path, json);
    }
}
