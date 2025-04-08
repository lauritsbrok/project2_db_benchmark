using System;
using Npgsql;
using DotNetEnv;
using System.Text;

namespace project2_db_benchmark.postgres;

public class DatabaseHelper
{
    public async Task InsertJsonFromFileInChunksAsync(string filePath, int chunkSize = 1000)
    {
        // Load the .env file
        Env.Load();

        // Get the values from the .env file
        string? dbUsername = Environment.GetEnvironmentVariable("POSTGRES_USER");
        string? dbPassword = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
        string? dbDatabase = Environment.GetEnvironmentVariable("POSTGRES_DB");

        // Construct the connection string
        var connectionString = $"Host=localhost;Username={dbUsername};Password={dbPassword};Database={dbDatabase}";

        // Open a connection to the PostgreSQL database
        using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        // Initialize a list to hold the JSON objects for the current chunk
        var jsonBatch = new List<string>();

        // Read lines from the file (efficient memory usage with ReadLines)
        var lines = File.ReadLines(filePath);
        int count = 0;

        // Prepare the SQL command for batched insert
        string createTablesSql = @"
            CREATE TABLE IF NOT EXISTS tip (
                text text,
                date text,
                compliment_count INT,
                business_id text,
                user_id text
            );
        ";
        using var cmd = new NpgsqlCommand(createTablesSql, conn);
        await cmd.ExecuteNonQueryAsync();

        foreach (var line in lines)
        {
            jsonBatch.Add(line); // Add the JSON object to the current batch

            if (jsonBatch.Count >= chunkSize)
            {
                // If the batch is full, insert it into the database
                await InsertBatchAsync(cmd, jsonBatch);

                // Clear the batch for the next chunk
                jsonBatch.Clear();
            }

            count++;
        }

        // If there are any remaining JSON blobs in the last chunk, insert them
        if (jsonBatch.Count > 0)
        {
            await InsertBatchAsync(cmd, jsonBatch);
        }

        Console.WriteLine($"Inserted {count} records.");
    }

    private async Task InsertBatchAsync(NpgsqlCommand cmd, List<string> jsonBatch)
    {
        // Build the SQL command for inserting the batch
        var sb = new StringBuilder("INSERT INTO tip (text, date, compliment_count, business_id, user_id) VALUES ");

        for (int i = 0; i < jsonBatch.Count; i++)
        {
            if (i > 0)
                sb.Append(", ");

            sb.Append($"(@text{i}, @date{i}, @compliment_count{i}, @business_id{i}, @user_id{i})");

            // Parse the JSON object
            var jsonObject = System.Text.Json.JsonDocument.Parse(jsonBatch[i]).RootElement;

            // Extract values from the JSON object
            string text = jsonObject.GetProperty("text").GetString() ?? string.Empty;
            string date = jsonObject.GetProperty("date").GetString() ?? string.Empty;
            int compliment_count = jsonObject.TryGetProperty("compliment_count", out var complimentCountElement) && complimentCountElement.TryGetInt32(out var count) ? count : 0;
            string business_id = jsonObject.GetProperty("business_id").GetString() ?? string.Empty;
            string user_id = jsonObject.GetProperty("user_id").GetString() ?? string.Empty;

            // Add parameters for each JSON object
            cmd.Parameters.AddWithValue($"text{i}", text);
            cmd.Parameters.AddWithValue($"date{i}", date);
            cmd.Parameters.AddWithValue($"compliment_count{i}", compliment_count);
            cmd.Parameters.AddWithValue($"business_id{i}", business_id);
            cmd.Parameters.AddWithValue($"user_id{i}", user_id);
        }

        cmd.CommandText = sb.ToString();

        // Execute the batch insert
        await cmd.ExecuteNonQueryAsync();

        // Clear the parameters for the next batch
        cmd.Parameters.Clear();
    }

    public async Task CreateTableAndInsertTestItemAsync()
    {
        // Load environment variables from the .env file
        Env.Load();

        // Get the database connection parameters from the environment variables
        string? dbUsername = Environment.GetEnvironmentVariable("POSTGRES_USER");
        string? dbPassword = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
        string? dbDatabase = Environment.GetEnvironmentVariable("POSTGRES_DB");

        // Construct the connection string
        var connectionString = $"Host=localhost;Username={dbUsername};Password={dbPassword};Database={dbDatabase}";

        // Open a connection to the PostgreSQL database
        using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        // Create the tables (if they don't exist)
        string createTablesSql = @"
            CREATE TABLE IF NOT EXISTS users (
                id serial PRIMARY KEY,
                name text
            );

            CREATE TABLE IF NOT EXISTS addresses (
                id serial PRIMARY KEY,
                user_id integer REFERENCES users(id),  -- Foreign key to the users table
                street text,
                city text
            );
        ";

        using (var cmd = new NpgsqlCommand(createTablesSql, conn))
        {
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine("Tables created successfully or already exist.");
        }

        // Insert a user into the 'users' table
        string userName = "Alice";  // Example name
        string insertUserSql = "INSERT INTO users (name) VALUES (@name) RETURNING id";
        int userId;

        using (var cmd = new NpgsqlCommand(insertUserSql, conn))
        {
            cmd.Parameters.AddWithValue("name", userName);
            userId = (int)await cmd.ExecuteScalarAsync();
            Console.WriteLine($"User inserted with ID: {userId}");
        }

        // Insert an address into the 'addresses' table (normalized example)
        string street = "123 Main St";
        string city = "Sample City";
        string insertAddressSql = "INSERT INTO addresses (user_id, street, city) VALUES (@user_id, @street, @city)";

        using (var cmd = new NpgsqlCommand(insertAddressSql, conn))
        {
            cmd.Parameters.AddWithValue("user_id", userId);
            cmd.Parameters.AddWithValue("street", street);
            cmd.Parameters.AddWithValue("city", city);
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine("Address inserted successfully.");
        }
    }
}
