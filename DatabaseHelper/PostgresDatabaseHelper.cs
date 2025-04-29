using System;
using Npgsql;
using DotNetEnv;
using System.Text;
using project2_db_benchmark.Models.Postgres;
using project2_db_benchmark.Models.Shared;

namespace project2_db_benchmark.DatabaseHelper;

public class PostgresDatabaseHelper
{
    public async Task InsertJsonFromFileInChunksAsync(string filePath, int chunkSize = 1000, string tableType = "tip")
    {
        // Construct the connection string
        var connectionString = $"Host=localhost;Username={Globals.POSTGRES_USER};Password={Globals.POSTGRES_USER};Database={Globals.POSTGRES_DB}";

        // Open a connection to the PostgreSQL database
        using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        // Initialize a list to hold the JSON objects for the current chunk
        var jsonBatch = new List<string>();

        // Read lines from the file (efficient memory usage with ReadLines)
        var lines = File.ReadLines(filePath);
        int count = 0;

        // Prepare the SQL command for batched insert based on table type
        string createTablesSql = GetCreateTableSql(tableType);
        using var cmd = new NpgsqlCommand(createTablesSql, conn);
        await cmd.ExecuteNonQueryAsync();

        foreach (var line in lines)
        {
            jsonBatch.Add(line); // Add the JSON object to the current batch

            if (jsonBatch.Count >= chunkSize)
            {
                // If the batch is full, insert it into the database
                await InsertBatchAsync(cmd, jsonBatch, tableType);

                // Clear the batch for the next chunk
                jsonBatch.Clear();
            }

            count++;
        }

        // If there are any remaining JSON blobs in the last chunk, insert them
        if (jsonBatch.Count > 0)
        {
            await InsertBatchAsync(cmd, jsonBatch, tableType);
        }

        Console.WriteLine($"Inserted {count} records into {tableType}.");
    }

    private string GetCreateTableSql(string tableType)
    {
        switch (tableType.ToLower())
        {
            case "tip":
                return @"
                    CREATE TABLE IF NOT EXISTS tip (
                        text text,
                        date text,
                        compliment_count INT,
                        business_id text,
                        user_id text
                    );";
            case "businesses":
                return @"
                    CREATE TABLE IF NOT EXISTS businesses (
                        business_id text PRIMARY KEY,
                        name text,
                        address text,
                        city text,
                        state text,
                        postal_code text,
                        latitude double precision,
                        longitude double precision,
                        stars double precision,
                        review_count int,
                        is_open int,
                        categories text
                    );";
            case "reviews":
                return @"
                    CREATE TABLE IF NOT EXISTS reviews (
                        review_id text PRIMARY KEY,
                        user_id text,
                        business_id text,
                        stars int,
                        date text,
                        text text,
                        useful int,
                        funny int,
                        cool int
                    );";
            default:
                return $"CREATE TABLE IF NOT EXISTS {tableType} (data jsonb);";
        }
    }

    private async Task InsertBatchAsync(NpgsqlCommand cmd, List<string> jsonBatch, string tableType = "tip")
    {
        // Build the SQL command for inserting the batch based on table type
        var sb = new StringBuilder();
        
        switch (tableType.ToLower())
        {
            case "tip":
                sb.Append("INSERT INTO tip (text, date, compliment_count, business_id, user_id) VALUES ");
                for (int i = 0; i < jsonBatch.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
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
                break;
                
            case "businesses":
                sb.Append("INSERT INTO businesses (business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, categories) VALUES ");
                for (int i = 0; i < jsonBatch.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append($"(@bid{i}, @name{i}, @address{i}, @city{i}, @state{i}, @postal{i}, @lat{i}, @lng{i}, @stars{i}, @review_count{i}, @is_open{i}, @categories{i})");

                    var jsonObject = System.Text.Json.JsonDocument.Parse(jsonBatch[i]).RootElement;
                    
                    cmd.Parameters.AddWithValue($"bid{i}", jsonObject.GetProperty("business_id").GetString() ?? string.Empty);
                    cmd.Parameters.AddWithValue($"name{i}", jsonObject.GetProperty("name").GetString() ?? string.Empty);
                    cmd.Parameters.AddWithValue($"address{i}", jsonObject.TryGetProperty("address", out var addr) ? addr.GetString() ?? string.Empty : string.Empty);
                    cmd.Parameters.AddWithValue($"city{i}", jsonObject.TryGetProperty("city", out var city) ? city.GetString() ?? string.Empty : string.Empty);
                    cmd.Parameters.AddWithValue($"state{i}", jsonObject.TryGetProperty("state", out var state) ? state.GetString() ?? string.Empty : string.Empty);
                    cmd.Parameters.AddWithValue($"postal{i}", jsonObject.TryGetProperty("postal_code", out var postal) ? postal.GetString() ?? string.Empty : string.Empty);
                    cmd.Parameters.AddWithValue($"lat{i}", jsonObject.TryGetProperty("latitude", out var lat) && lat.TryGetDouble(out var latVal) ? latVal : 0.0);
                    cmd.Parameters.AddWithValue($"lng{i}", jsonObject.TryGetProperty("longitude", out var lng) && lng.TryGetDouble(out var lngVal) ? lngVal : 0.0);
                    cmd.Parameters.AddWithValue($"stars{i}", jsonObject.TryGetProperty("stars", out var stars) && stars.TryGetDouble(out var starsVal) ? starsVal : 0.0);
                    cmd.Parameters.AddWithValue($"review_count{i}", jsonObject.TryGetProperty("review_count", out var revCount) && revCount.TryGetInt32(out var revCountVal) ? revCountVal : 0);
                    cmd.Parameters.AddWithValue($"is_open{i}", jsonObject.TryGetProperty("is_open", out var isOpen) && isOpen.TryGetInt32(out var isOpenVal) ? isOpenVal : 0);
                    cmd.Parameters.AddWithValue($"categories{i}", jsonObject.TryGetProperty("categories", out var cats) ? cats.GetString() ?? string.Empty : string.Empty);
                }
                break;
                
            case "reviews":
                sb.Append("INSERT INTO reviews (review_id, user_id, business_id, stars, date, text, useful, funny, cool) VALUES ");
                for (int i = 0; i < jsonBatch.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append($"(@rid{i}, @uid{i}, @bid{i}, @stars{i}, @date{i}, @text{i}, @useful{i}, @funny{i}, @cool{i})");

                    var jsonObject = System.Text.Json.JsonDocument.Parse(jsonBatch[i]).RootElement;
                    
                    cmd.Parameters.AddWithValue($"rid{i}", jsonObject.GetProperty("review_id").GetString() ?? string.Empty);
                    cmd.Parameters.AddWithValue($"uid{i}", jsonObject.GetProperty("user_id").GetString() ?? string.Empty);
                    cmd.Parameters.AddWithValue($"bid{i}", jsonObject.GetProperty("business_id").GetString() ?? string.Empty);
                    cmd.Parameters.AddWithValue($"stars{i}", jsonObject.TryGetProperty("stars", out var stars) && stars.TryGetInt32(out var starsVal) ? starsVal : 0);
                    cmd.Parameters.AddWithValue($"date{i}", jsonObject.GetProperty("date").GetString() ?? string.Empty);
                    cmd.Parameters.AddWithValue($"text{i}", jsonObject.GetProperty("text").GetString() ?? string.Empty);
                    cmd.Parameters.AddWithValue($"useful{i}", jsonObject.TryGetProperty("useful", out var useful) && useful.TryGetInt32(out var usefulVal) ? usefulVal : 0);
                    cmd.Parameters.AddWithValue($"funny{i}", jsonObject.TryGetProperty("funny", out var funny) && funny.TryGetInt32(out var funnyVal) ? funnyVal : 0);
                    cmd.Parameters.AddWithValue($"cool{i}", jsonObject.TryGetProperty("cool", out var cool) && cool.TryGetInt32(out var coolVal) ? coolVal : 0);
                }
                break;
                
            default:
                sb.Append($"INSERT INTO {tableType} (data) VALUES ");
                for (int i = 0; i < jsonBatch.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append($"(@data{i}::jsonb)");
                    cmd.Parameters.AddWithValue($"data{i}", jsonBatch[i]);
                }
                break;
        }

        cmd.CommandText = sb.ToString();

        // Execute the batch insert
        await cmd.ExecuteNonQueryAsync();

        // Clear the parameters for the next batch
        cmd.Parameters.Clear();
    }

    public async Task<List<Dictionary<string, object>>> ExecuteQueryAsync(string sql)
    {
        // Load the .env file
        Env.Load();

        // Get the database connection parameters
        string? dbUsername = Environment.GetEnvironmentVariable("POSTGRES_USER");
        string? dbPassword = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
        string? dbDatabase = Environment.GetEnvironmentVariable("POSTGRES_DB");

        // Construct the connection string
        var connectionString = $"Host=localhost;Username={dbUsername};Password={dbPassword};Database={dbDatabase}";

        // Results list
        var results = new List<Dictionary<string, object>>();

        // Execute the query
        using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        
        using var cmd = new NpgsqlCommand(sql, conn);
        using var reader = await cmd.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                var value = reader.GetValue(i);
                row[name] = value == DBNull.Value ? null : value;
            }
            results.Add(row);
        }

        Console.WriteLine($"Query executed. Returned {results.Count} rows.");
        return results;
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

    internal async Task InsertBusinessAsync(Business business)
    {
        throw new NotImplementedException();
    }

    internal async Task InsertCheckinAsync(Checkin checkin)
    {
        throw new NotImplementedException();
    }

    internal async Task InsertReviewAsync(Review review)
    {
        throw new NotImplementedException();
    }

    internal async Task InsertTipAsync(Tip tip)
    {
        throw new NotImplementedException();
    }

    internal async Task InsertUserAsync(User user)
    {
        throw new NotImplementedException();
    }

    internal async Task InsertPhotoAsync(Photo photo)
    {
        throw new NotImplementedException();
    }
}
