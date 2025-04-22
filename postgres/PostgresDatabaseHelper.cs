using System;
using Npgsql;
using DotNetEnv;
using System.Text;
using System.Text.Json;
using project2_db_benchmark.postgres.Models;

namespace project2_db_benchmark.postgres;

public class PostgresDatabaseHelper
{
    public PostgresDatabaseHelper()
    {
        // Don't automatically setup indexes in the constructor
        // We'll call this explicitly after tables are created
    }

    public async Task InsertJsonFromFileInChunksAsync(string filePath, int chunkSize = 1000, string tableType = "tip")
    {
        // Load the .env file
        Env.Load();

        // Get the values from the .env file
        string? dbUsername = Environment.GetEnvironmentVariable("POSTGRES_USER");
        string? dbPassword = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
        string? dbDatabase = Environment.GetEnvironmentVariable("POSTGRES_DB");

        // Construct the connection string
        var connectionString = $"Host=localhost;Port=5433;Username={dbUsername};Password={dbPassword};Database={dbDatabase}";

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
            case "business":
                return @"
                    CREATE TABLE IF NOT EXISTS business (
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
                        attributes jsonb,
                        categories text,
                        hours jsonb
                    );";
            case "review":
                return @"
                    CREATE TABLE IF NOT EXISTS review (
                        review_id text PRIMARY KEY,
                        user_id text,
                        business_id text,
                        stars double precision,
                        date text,
                        text text,
                        useful int,
                        funny int,
                        cool int
                    );";
            case "users":
                return @"
                    CREATE TABLE IF NOT EXISTS ""users"" (
                        ""user_id"" text PRIMARY KEY,
                        ""name"" text,
                        ""review_count"" int,
                        ""yelping_since"" text,
                        ""friends"" text[],
                        ""useful"" int,
                        ""funny"" int,
                        ""cool"" int,
                        ""fans"" int,
                        ""elite"" int[],
                        ""average_stars"" double precision,
                        ""compliment_hot"" int,
                        ""compliment_more"" int,
                        ""compliment_profile"" int,
                        ""compliment_cute"" int,
                        ""compliment_list"" int,
                        ""compliment_note"" int,
                        ""compliment_plain"" int,
                        ""compliment_cool"" int,
                        ""compliment_funny"" int,
                        ""compliment_writer"" int,
                        ""compliment_photos"" int
                    );";
            case "checkin":
                return @"
                    CREATE TABLE IF NOT EXISTS checkin (
                        business_id text,
                        date text
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
                    var tip = JsonSerializer.Deserialize<Tip>(jsonBatch[i]);

                    // Add parameters for each JSON object
                    cmd.Parameters.AddWithValue($"text{i}", tip.Text);
                    cmd.Parameters.AddWithValue($"date{i}", tip.Date);
                    cmd.Parameters.AddWithValue($"compliment_count{i}", tip.ComplimentCount);
                    cmd.Parameters.AddWithValue($"business_id{i}", tip.BusinessId);
                    cmd.Parameters.AddWithValue($"user_id{i}", tip.UserId);
                }
                break;

            case "business":
                sb.Append("INSERT INTO business (business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, attributes, categories, hours) VALUES ");
                for (int i = 0; i < jsonBatch.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append($"(@bid{i}, @name{i}, @address{i}, @city{i}, @state{i}, @postal{i}, @lat{i}, @lng{i}, @stars{i}, @review_count{i}, @is_open{i}, @attributes{i}, @categories{i}, @hours{i})");

                    var business = JsonSerializer.Deserialize<Business>(jsonBatch[i]);

                    cmd.Parameters.AddWithValue($"bid{i}", business.BusinessId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"name{i}", business.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"address{i}", business.Address ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"city{i}", business.City ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"state{i}", business.State ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"postal{i}", business.PostalCode ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"lat{i}", business.Latitude != 0 ? business.Latitude : (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"lng{i}", business.Longitude != 0 ? business.Longitude : (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"stars{i}", business.Stars != 0 ? business.Stars : (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"review_count{i}", business.ReviewCount != 0 ? business.ReviewCount : (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"is_open{i}", business.IsOpen != 0 ? business.IsOpen : (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"attributes{i}", NpgsqlTypes.NpgsqlDbType.Jsonb, business.Attributes != null ? JsonSerializer.Serialize(business.Attributes) : (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"categories{i}", !string.IsNullOrEmpty(business.Categories) ? business.Categories : (object)DBNull.Value);
                    cmd.Parameters.AddWithValue($"hours{i}", NpgsqlTypes.NpgsqlDbType.Jsonb, business.Hours != null ? JsonSerializer.Serialize(business.Hours) : (object)DBNull.Value);
                }
                break;

            case "review":
                sb.Append("INSERT INTO review (review_id, user_id, business_id, stars, date, text, useful, funny, cool) VALUES ");
                for (int i = 0; i < jsonBatch.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append($"(@rid{i}, @uid{i}, @bid{i}, @stars{i}, @date{i}, @text{i}, @useful{i}, @funny{i}, @cool{i})");

                    var review = JsonSerializer.Deserialize<Review>(jsonBatch[i]);

                    cmd.Parameters.AddWithValue($"rid{i}", review.ReviewId);
                    cmd.Parameters.AddWithValue($"uid{i}", review.UserId);
                    cmd.Parameters.AddWithValue($"bid{i}", review.BusinessId);
                    cmd.Parameters.AddWithValue($"stars{i}", review.Stars);
                    cmd.Parameters.AddWithValue($"date{i}", review.Date);
                    cmd.Parameters.AddWithValue($"text{i}", review.Text);
                    cmd.Parameters.AddWithValue($"useful{i}", review.Useful);
                    cmd.Parameters.AddWithValue($"funny{i}", review.Funny);
                    cmd.Parameters.AddWithValue($"cool{i}", review.Cool);
                }
                break;

            case "users":
                sb.Append("INSERT INTO \"users\" (\"user_id\", \"name\", \"review_count\", \"yelping_since\", \"friends\", \"useful\", \"funny\", \"cool\", \"fans\", \"elite\", \"average_stars\", \"compliment_hot\", \"compliment_more\", \"compliment_profile\", \"compliment_cute\", \"compliment_list\", \"compliment_note\", \"compliment_plain\", \"compliment_cool\", \"compliment_funny\", \"compliment_writer\", \"compliment_photos\") VALUES ");
                for (int i = 0; i < jsonBatch.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append($"(@uid{i}, @name{i}, @review_count{i}, @yelping_since{i}, @friends{i}, @useful{i}, @funny{i}, @cool{i}, @fans{i}, @elite{i}, @average_stars{i}, @compliment_hot{i}, @compliment_more{i}, @compliment_profile{i}, @compliment_cute{i}, @compliment_list{i}, @compliment_note{i}, @compliment_plain{i}, @compliment_cool{i}, @compliment_funny{i}, @compliment_writer{i}, @compliment_photos{i})");

                    var user = JsonSerializer.Deserialize<User>(jsonBatch[i]);

                    cmd.Parameters.AddWithValue($"uid{i}", user.UserId);
                    cmd.Parameters.AddWithValue($"name{i}", user.Name);
                    cmd.Parameters.AddWithValue($"review_count{i}", user.ReviewCount);
                    cmd.Parameters.AddWithValue($"yelping_since{i}", user.YelpingSince);
                    cmd.Parameters.AddWithValue($"friends{i}", user.Friends.ToArray());
                    cmd.Parameters.AddWithValue($"useful{i}", user.Useful);
                    cmd.Parameters.AddWithValue($"funny{i}", user.Funny);
                    cmd.Parameters.AddWithValue($"cool{i}", user.Cool);
                    cmd.Parameters.AddWithValue($"fans{i}", user.Fans);
                    cmd.Parameters.AddWithValue($"elite{i}", user.Elite.ToArray());
                    cmd.Parameters.AddWithValue($"average_stars{i}", user.AverageStars);
                    cmd.Parameters.AddWithValue($"compliment_hot{i}", user.ComplimentHot);
                    cmd.Parameters.AddWithValue($"compliment_more{i}", user.ComplimentMore);
                    cmd.Parameters.AddWithValue($"compliment_profile{i}", user.ComplimentProfile);
                    cmd.Parameters.AddWithValue($"compliment_cute{i}", user.ComplimentCute);
                    cmd.Parameters.AddWithValue($"compliment_list{i}", user.ComplimentList);
                    cmd.Parameters.AddWithValue($"compliment_note{i}", user.ComplimentNote);
                    cmd.Parameters.AddWithValue($"compliment_plain{i}", user.ComplimentPlain);
                    cmd.Parameters.AddWithValue($"compliment_cool{i}", user.ComplimentCool);
                    cmd.Parameters.AddWithValue($"compliment_funny{i}", user.ComplimentFunny);
                    cmd.Parameters.AddWithValue($"compliment_writer{i}", user.ComplimentWriter);
                    cmd.Parameters.AddWithValue($"compliment_photos{i}", user.ComplimentPhotos);
                }
                break;

            case "checkin":
                sb.Append("INSERT INTO checkin (business_id, date) VALUES ");
                for (int i = 0; i < jsonBatch.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append($"(@bid{i}, @date{i})");

                    var checkin = JsonSerializer.Deserialize<Checkin>(jsonBatch[i]);

                    cmd.Parameters.AddWithValue($"bid{i}", checkin.BusinessId);
                    cmd.Parameters.AddWithValue($"date{i}", checkin.Date);
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
        var connectionString = $"Host=localhost;Port=5433;Username={dbUsername};Password={dbPassword};Database={dbDatabase}";

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
        var connectionString = $"Host=localhost;Port=5433;Username={dbUsername};Password={dbPassword};Database={dbDatabase}";

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

    public async Task SetupIndexesAsync()
    {
        // Load the .env file
        Env.Load();

        // Get the database connection parameters
        string? dbUsername = Environment.GetEnvironmentVariable("POSTGRES_USER");
        string? dbPassword = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
        string? dbDatabase = Environment.GetEnvironmentVariable("POSTGRES_DB");

        // Construct the connection string
        var connectionString = $"Host=localhost;Port=5433;Username={dbUsername};Password={dbPassword};Database={dbDatabase}";

        // Open a connection to the PostgreSQL database
        using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        // Check if tables exist before creating indexes
        string[] tableTypes = { "business", "review", "users", "tip", "checkin" };
        foreach (var tableType in tableTypes)
        {
            // Check if the table exists
            string checkTableSql = $"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tableType}')";
            using (var cmd = new NpgsqlCommand(checkTableSql, conn))
            {
                bool tableExists = (bool)await cmd.ExecuteScalarAsync();
                if (!tableExists)
                {
                    Console.WriteLine($"Table '{tableType}' does not exist. Skipping index creation.");
                    continue;
                }
            }

            Console.WriteLine($"Creating indexes for {tableType}...");
        }

        // Create indexes for businesses
        string businessIndexesSql = @"
            -- Business indexes
            CREATE INDEX IF NOT EXISTS idx_businesses_location ON business USING gist (ll_to_earth(latitude, longitude));
            CREATE INDEX IF NOT EXISTS idx_businesses_city ON business (city);
            CREATE INDEX IF NOT EXISTS idx_businesses_stars ON business (stars DESC);
            CREATE INDEX IF NOT EXISTS idx_businesses_review_count ON business (review_count DESC);
        ";

        // Create indexes for reviews
        string reviewIndexesSql = @"
            -- Review indexes
            CREATE INDEX IF NOT EXISTS idx_reviews_business_id ON review (business_id);
            CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON review (user_id);
            CREATE INDEX IF NOT EXISTS idx_reviews_date ON review (date DESC);
        ";

        // Create indexes for users
        string userIndexesSql = @"
            -- User indexes
            CREATE INDEX IF NOT EXISTS idx_users_name ON users (name);
            -- Note: LastActive is not in our current schema, so we'll skip that index
        ";

        // Create indexes for checkins
        string checkinIndexesSql = @"
            -- Checkin indexes
            CREATE INDEX IF NOT EXISTS idx_checkins_business_id ON checkin (business_id);
        ";

        // Create indexes for tips
        string tipIndexesSql = @"
            -- Tip indexes
            CREATE INDEX IF NOT EXISTS idx_tips_business_id ON tip (business_id);
            CREATE INDEX IF NOT EXISTS idx_tips_user_id ON tip (user_id);
            CREATE INDEX IF NOT EXISTS idx_tips_date ON tip (date DESC);
        ";

        // Execute the SQL to create indexes
        try
        {
            using (var cmd = new NpgsqlCommand(businessIndexesSql, conn))
            {
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine("Business indexes created successfully.");
            }

            using (var cmd = new NpgsqlCommand(reviewIndexesSql, conn))
            {
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine("Review indexes created successfully.");
            }

            using (var cmd = new NpgsqlCommand(userIndexesSql, conn))
            {
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine("User indexes created successfully.");
            }

            using (var cmd = new NpgsqlCommand(checkinIndexesSql, conn))
            {
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine("Checkin indexes created successfully.");
            }

            using (var cmd = new NpgsqlCommand(tipIndexesSql, conn))
            {
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine("Tip indexes created successfully.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error creating indexes: {ex.Message}");
        }
    }

    public async Task CreateAllTablesAsync()
    {
        // Load the .env file
        Env.Load();

        // Get the database connection parameters
        string? dbUsername = Environment.GetEnvironmentVariable("POSTGRES_USER");
        string? dbPassword = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
        string? dbDatabase = Environment.GetEnvironmentVariable("POSTGRES_DB");

        // Construct the connection string
        var connectionString = $"Host=localhost;Port=5433;Username={dbUsername};Password={dbPassword};Database={dbDatabase}";

        // Open a connection to the PostgreSQL database
        using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        // Create tables for each entity type
        string[] tableTypes = { "business", "review", "users", "tip", "checkin" };

        foreach (var tableType in tableTypes)
        {
            Console.WriteLine($"Creating table for {tableType}...");
            string createTableSql = GetCreateTableSql(tableType);

            using var cmd = new NpgsqlCommand(createTableSql, conn);
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine($"Table for {tableType} created successfully.");
        }

        Console.WriteLine("All tables created successfully.");
    }
}
