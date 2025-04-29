using System;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using project2_db_benchmark.Models.Postgres;
using project2_db_benchmark.Models.Shared;

namespace project2_db_benchmark.DatabaseHelper;

public class PostgresDatabaseHelper : IDisposable
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgresDatabaseHelper()
    {
        var dataSourceBuilder = new NpgsqlDataSourceBuilder($"Host=localhost;Port=5433;Username={Globals.POSTGRES_USER};Password={Globals.POSTGRES_PASSWORD};Database={Globals.POSTGRES_DB}");
        dataSourceBuilder.EnableDynamicJson();
        _dataSource = dataSourceBuilder.Build();

        // Drop and recreate database tables
        InitializeDatabase().Wait();
    }

    private async Task InitializeDatabase()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        // Drop existing tables if they exist
        cmd.CommandText = @"
            DROP TABLE IF EXISTS photo CASCADE;
            DROP TABLE IF EXISTS tip CASCADE;
            DROP TABLE IF EXISTS checkin CASCADE;
            DROP TABLE IF EXISTS review CASCADE;
            DROP TABLE IF EXISTS users CASCADE;
            DROP TABLE IF EXISTS business CASCADE;
        ";
        await cmd.ExecuteNonQueryAsync();

        // Create tables
        cmd.CommandText = @"
            CREATE TABLE business (
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
            );

            CREATE TABLE IF NOT EXISTS ""users"" (
                ""user_id"" text PRIMARY KEY,
                ""name"" text,
                ""review_count"" int,
                ""yelping_since"" text,
                ""friends"" text,
                ""useful"" int,
                ""funny"" int,
                ""cool"" int,
                ""fans"" int,
                ""elite"" text,
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
            );

            CREATE TABLE review (
                review_id text PRIMARY KEY,
                user_id text,
                business_id text,
                stars double precision,
                date text,
                text text,
                useful int,
                funny int,
                cool int
            );

            CREATE TABLE checkin (
                business_id text,
                date text
            );

            CREATE TABLE tip (
                text text,
                date text,
                compliment_count INT,
                business_id text,
                user_id text
            );

            CREATE TABLE photo (
                photo_id VARCHAR(255) PRIMARY KEY,
                business_id VARCHAR(255) NOT NULL,
                caption TEXT NOT NULL,
                label VARCHAR(255) NOT NULL,
                FOREIGN KEY (business_id) REFERENCES business(business_id)
            );

            CREATE INDEX idx_businesses_categories ON business(categories);
            CREATE INDEX idx_businesses_city ON business(city);
            CREATE INDEX idx_businesses_stars ON business(stars);
            CREATE INDEX idx_reviews_business_id ON review(business_id);
            CREATE INDEX idx_reviews_user_id ON review(user_id);
            CREATE INDEX idx_reviews_date ON review(date);
            CREATE INDEX idx_users_name ON users(name);
            CREATE INDEX idx_checkins_business_id ON checkin(business_id);
            CREATE INDEX idx_tips_business_id ON tip(business_id);
            CREATE INDEX idx_tips_user_id ON tip(user_id);
            CREATE INDEX idx_tips_date ON tip(date);
            CREATE INDEX idx_photos_business_id ON photo(business_id);
        ";
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task InsertBusinessAsync(Business business)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            INSERT INTO business (
                business_id, name, address, city, state, postal_code,
                latitude, longitude, stars, review_count, is_open,
                attributes, categories, hours
            )
            VALUES (
                @business_id, @name, @address, @city, @state, @postal_code,
                @latitude, @longitude, @stars, @review_count, @is_open,
                @attributes::jsonb, @categories, @hours::jsonb
            )
            ON CONFLICT (business_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                postal_code = EXCLUDED.postal_code,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                stars = EXCLUDED.stars,
                review_count = EXCLUDED.review_count,
                is_open = EXCLUDED.is_open,
                attributes = EXCLUDED.attributes,
                categories = EXCLUDED.categories,
                hours = EXCLUDED.hours;";

        cmd.Parameters.AddWithValue("business_id", business.BusinessId);
        cmd.Parameters.AddWithValue("name", business.Name);
        cmd.Parameters.AddWithValue("address", business.Address);
        cmd.Parameters.AddWithValue("city", business.City);
        cmd.Parameters.AddWithValue("state", business.State);
        cmd.Parameters.AddWithValue("postal_code", business.PostalCode);
        cmd.Parameters.AddWithValue("latitude", (object?)business.Latitude ?? DBNull.Value);
        cmd.Parameters.AddWithValue("longitude", (object?)business.Longitude ?? DBNull.Value);
        cmd.Parameters.AddWithValue("stars", (object?)business.Stars ?? DBNull.Value);
        cmd.Parameters.AddWithValue("review_count", (object?)business.ReviewCount ?? DBNull.Value);
        cmd.Parameters.AddWithValue("is_open", (object?)business.IsOpen ?? DBNull.Value);
        cmd.Parameters.AddWithValue("attributes", business.Attributes != null ? JsonSerializer.Serialize(business.Attributes) : DBNull.Value);
        cmd.Parameters.AddWithValue("categories", (object?)business.Categories ?? DBNull.Value);
        cmd.Parameters.AddWithValue("hours", business.Hours != null ? JsonSerializer.Serialize(business.Hours) : DBNull.Value);

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task InsertReviewAsync(Review review)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            INSERT INTO review (
                review_id, user_id, business_id, stars, date,
                text, useful, funny, cool
            )
            VALUES (
                @review_id, @user_id, @business_id, @stars, @date,
                @text, @useful, @funny, @cool
            );";

        cmd.Parameters.AddWithValue("review_id", review.ReviewId);
        cmd.Parameters.AddWithValue("user_id", review.UserId);
        cmd.Parameters.AddWithValue("business_id", review.BusinessId);
        cmd.Parameters.AddWithValue("stars", review.Stars);
        cmd.Parameters.AddWithValue("date", review.Date);
        cmd.Parameters.AddWithValue("text", (object?)review.Text ?? DBNull.Value);
        cmd.Parameters.AddWithValue("useful", (object?)review.Useful ?? DBNull.Value);
        cmd.Parameters.AddWithValue("funny", (object?)review.Funny ?? DBNull.Value);
        cmd.Parameters.AddWithValue("cool", (object?)review.Cool ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task InsertUserAsync(User user)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            INSERT INTO users (
                user_id, name, review_count, yelping_since, friends,
                useful, funny, cool, fans, elite, average_stars,
                compliment_hot, compliment_more, compliment_profile,
                compliment_cute, compliment_list, compliment_note,
                compliment_plain, compliment_cool, compliment_funny,
                compliment_writer, compliment_photos
            )
            VALUES (
                @user_id, @name, @review_count, @yelping_since, @friends,
                @useful, @funny, @cool, @fans, @elite, @average_stars,
                @compliment_hot, @compliment_more, @compliment_profile,
                @compliment_cute, @compliment_list, @compliment_note,
                @compliment_plain, @compliment_cool, @compliment_funny,
                @compliment_writer, @compliment_photos
            );";

        cmd.Parameters.AddWithValue("user_id", user.UserId);
        cmd.Parameters.AddWithValue("name", user.Name);
        cmd.Parameters.AddWithValue("review_count", (object?)user.ReviewCount ?? DBNull.Value);
        cmd.Parameters.AddWithValue("yelping_since", (object?)user.YelpingSince ?? DBNull.Value);
        cmd.Parameters.AddWithValue("friends", (object?)user.Friends ?? DBNull.Value);
        cmd.Parameters.AddWithValue("useful", (object?)user.Useful ?? DBNull.Value);
        cmd.Parameters.AddWithValue("funny", (object?)user.Funny ?? DBNull.Value);
        cmd.Parameters.AddWithValue("cool", (object?)user.Cool ?? DBNull.Value);
        cmd.Parameters.AddWithValue("fans", (object?)user.Fans ?? DBNull.Value);
        cmd.Parameters.AddWithValue("elite", (object?)user.Elite ?? DBNull.Value);
        cmd.Parameters.AddWithValue("average_stars", (object?)user.AverageStars ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_hot", (object?)user.ComplimentHot ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_more", (object?)user.ComplimentMore ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_profile", (object?)user.ComplimentProfile ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_cute", (object?)user.ComplimentCute ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_list", (object?)user.ComplimentList ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_note", (object?)user.ComplimentNote ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_plain", (object?)user.ComplimentPlain ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_cool", (object?)user.ComplimentCool ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_funny", (object?)user.ComplimentFunny ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_writer", (object?)user.ComplimentWriter ?? DBNull.Value);
        cmd.Parameters.AddWithValue("compliment_photos", (object?)user.ComplimentPhotos ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task InsertCheckinAsync(Checkin checkin)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            INSERT INTO checkin (business_id, date)
            VALUES (@business_id, @date);";

        cmd.Parameters.AddWithValue("business_id", checkin.BusinessId);
        cmd.Parameters.AddWithValue("date", checkin.Date);

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task InsertTipAsync(Tip tip)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            INSERT INTO tip (
                business_id, user_id, date, text, compliment_count
            )
            VALUES (
                @business_id, @user_id, @date, @text, @compliment_count
            );";

        cmd.Parameters.AddWithValue("business_id", tip.BusinessId);
        cmd.Parameters.AddWithValue("user_id", tip.UserId);
        cmd.Parameters.AddWithValue("date", tip.Date);
        cmd.Parameters.AddWithValue("text", tip.Text);
        cmd.Parameters.AddWithValue("compliment_count", tip.ComplimentCount);

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task InsertPhotoAsync(Photo photo)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            INSERT INTO photo (
                photo_id, business_id, caption, label
            )
            VALUES (
                @photo_id, @business_id, @caption, @label
            );";

        cmd.Parameters.AddWithValue("photo_id", photo.PhotoId);
        cmd.Parameters.AddWithValue("business_id", photo.BusinessId);
        cmd.Parameters.AddWithValue("caption", photo.Caption);
        cmd.Parameters.AddWithValue("label", photo.Label);

        await cmd.ExecuteNonQueryAsync();
    }

    public void Dispose()
    {
        _dataSource?.Dispose();
    }
}
