using System;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using project2_db_benchmark.Models;

namespace project2_db_benchmark.DatabaseHelper;

#pragma warning disable CS8601, CS8603

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
        cmd.Parameters.AddWithValue("attributes", business.RawAttributes.ValueKind != System.Text.Json.JsonValueKind.Undefined ? 
                                    business.RawAttributes.GetRawText() : DBNull.Value);
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

    public async Task<List<Tip>> GetTipsByBusinessIdAsync(string businessId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT text, date, compliment_count, business_id, user_id
            FROM tip
            WHERE business_id = @business_id";

        cmd.Parameters.AddWithValue("business_id", businessId);

        var tips = new List<Tip>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            tips.Add(new Tip
            {
                Text = reader.GetString(0),
                Date = reader.GetString(1),
                ComplimentCount = reader.GetInt32(2),
                BusinessId = reader.GetString(3),
                UserId = reader.GetString(4)
            });
        }

        return tips;
    }

    public async Task<Business> GetBusinessByIdAsync(string businessId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT business_id, name, address, city, state, postal_code,
                   latitude, longitude, stars, review_count, is_open,
                   attributes, categories, hours
            FROM business
            WHERE business_id = @business_id";

        cmd.Parameters.AddWithValue("business_id", businessId);

        await using var reader = await cmd.ExecuteReaderAsync();

        if (await reader.ReadAsync())
        {
            var business = new Business
            {
                BusinessId = reader.GetString(0),
                Name = reader.GetString(1),
                Address = !reader.IsDBNull(2) ? reader.GetString(2) : null,
                City = !reader.IsDBNull(3) ? reader.GetString(3) : null,
                State = !reader.IsDBNull(4) ? reader.GetString(4) : null,
                PostalCode = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                Latitude = !reader.IsDBNull(6) ? reader.GetDouble(6) : null,
                Longitude = !reader.IsDBNull(7) ? reader.GetDouble(7) : null,
                Stars = !reader.IsDBNull(8) ? reader.GetDouble(8) : null,
                ReviewCount = !reader.IsDBNull(9) ? reader.GetInt32(9) : null,
                IsOpen = !reader.IsDBNull(10) ? reader.GetInt32(10) : null,
                Categories = !reader.IsDBNull(12) ? reader.GetString(12) : null
            };

            // Handle attributes JSON
            if (!reader.IsDBNull(11))
            {
                string attributesJson = reader.GetString(11);
                business.RawAttributes = JsonDocument.Parse(attributesJson).RootElement;
            }

            // Handle hours
            if (!reader.IsDBNull(13))
            {
                string hoursJson = reader.GetString(13);
                business.Hours = JsonSerializer.Deserialize<Dictionary<string, string>>(hoursJson);
            }

            return business;
        }

        return null;
    }

    public async Task<List<Review>> GetReviewsByIdAsync(string reviewId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT review_id, user_id, business_id, stars, date,
                   text, useful, funny, cool
            FROM review
            WHERE review_id = @review_id";

        cmd.Parameters.AddWithValue("review_id", reviewId);

        var reviews = new List<Review>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            reviews.Add(new Review
            {
                ReviewId = reader.GetString(0),
                UserId = reader.GetString(1),
                BusinessId = reader.GetString(2),
                Stars = reader.GetDouble(3),
                Date = reader.GetString(4),
                Text = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                Useful = !reader.IsDBNull(6) ? reader.GetInt32(6) : null,
                Funny = !reader.IsDBNull(7) ? reader.GetInt32(7) : null,
                Cool = !reader.IsDBNull(8) ? reader.GetInt32(8) : null
            });
        }

        return reviews;
    }

    public async Task<User> GetUserByIdAsync(string userId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT user_id, name, review_count, yelping_since, friends,
                   useful, funny, cool, fans, elite, average_stars,
                   compliment_hot, compliment_more, compliment_profile,
                   compliment_cute, compliment_list, compliment_note,
                   compliment_plain, compliment_cool, compliment_funny,
                   compliment_writer, compliment_photos
            FROM users
            WHERE user_id = @user_id";

        cmd.Parameters.AddWithValue("user_id", userId);

        await using var reader = await cmd.ExecuteReaderAsync();

        if (await reader.ReadAsync())
        {
            return new User
            {
                UserId = reader.GetString(0),
                Name = reader.GetString(1),
                // Additional properties omitted for brevity but would be included in a full implementation
            };
        }

        return null;
    }

    public async Task<List<Checkin>> GetCheckinsByBusinessIdAsync(string businessId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT business_id, date
            FROM checkin
            WHERE business_id = @business_id";

        cmd.Parameters.AddWithValue("business_id", businessId);

        var checkins = new List<Checkin>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            checkins.Add(new Checkin
            {
                BusinessId = reader.GetString(0),
                Date = reader.GetString(1)
            });
        }

        return checkins;
    }

    public async Task<Photo> GetPhotoByIdAsync(string photoId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT photo_id, business_id, caption, label
            FROM photo
            WHERE photo_id = @photo_id";

        cmd.Parameters.AddWithValue("photo_id", photoId);

        await using var reader = await cmd.ExecuteReaderAsync();

        if (await reader.ReadAsync())
        {
            return new Photo
            {
                PhotoId = reader.GetString(0),
                BusinessId = reader.GetString(1),
                Caption = reader.GetString(2),
                Label = reader.GetString(3)
            };
        }

        return null;
    }

    public async Task<List<Business>> GetAllBusinessesAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"SELECT * FROM business";

        var businesses = new List<Business>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var business = new Business
            {
                BusinessId = reader.GetString(0),
                Name = reader.GetString(1),
                Address = !reader.IsDBNull(2) ? reader.GetString(2) : null,
                City = !reader.IsDBNull(3) ? reader.GetString(3) : null,
                State = !reader.IsDBNull(4) ? reader.GetString(4) : null,
                PostalCode = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                Latitude = !reader.IsDBNull(6) ? reader.GetDouble(6) : null,
                Longitude = !reader.IsDBNull(7) ? reader.GetDouble(7) : null,
                Stars = !reader.IsDBNull(8) ? reader.GetDouble(8) : null,
                ReviewCount = !reader.IsDBNull(9) ? reader.GetInt32(9) : null,
                IsOpen = !reader.IsDBNull(10) ? reader.GetInt32(10) : null,
                Categories = !reader.IsDBNull(12) ? reader.GetString(12) : null
            };

            // Handle attributes JSON
            if (!reader.IsDBNull(11))
            {
                string attributesJson = reader.GetString(11);
                business.RawAttributes = JsonDocument.Parse(attributesJson).RootElement;
            }

            // Handle hours
            if (!reader.IsDBNull(13))
            {
                string hoursJson = reader.GetString(13);
                business.Hours = JsonSerializer.Deserialize<Dictionary<string, string>>(hoursJson);
            }

            businesses.Add(business);
        }

        return businesses;
    }

    public async Task<List<Review>> GetAllReviewsAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"SELECT * FROM review";

        var reviews = new List<Review>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            reviews.Add(new Review
            {
                ReviewId = reader.GetString(0),
                UserId = reader.GetString(1),
                BusinessId = reader.GetString(2),
                Stars = reader.GetDouble(3),
                Date = reader.GetString(4),
                Text = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                Useful = !reader.IsDBNull(6) ? reader.GetInt32(6) : null,
                Funny = !reader.IsDBNull(7) ? reader.GetInt32(7) : null,
                Cool = !reader.IsDBNull(8) ? reader.GetInt32(8) : null
            });
        }

        return reviews;
    }

    public async Task<List<User>> GetAllUsersAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"SELECT * FROM users";

        var users = new List<User>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            users.Add(new User
            {
                UserId = reader.GetString(0),
                Name = reader.GetString(1),
                ReviewCount = !reader.IsDBNull(2) ? reader.GetInt32(2) : null,
                YelpingSince = !reader.IsDBNull(3) ? reader.GetString(3) : null,
                Friends = !reader.IsDBNull(4) ? reader.GetString(4) : null,
                Useful = !reader.IsDBNull(5) ? reader.GetInt32(5) : null,
                Funny = !reader.IsDBNull(6) ? reader.GetInt32(6) : null,
                Cool = !reader.IsDBNull(7) ? reader.GetInt32(7) : null,
                Fans = !reader.IsDBNull(8) ? reader.GetInt32(8) : null,
                Elite = !reader.IsDBNull(9) ? reader.GetString(9) : null,
                AverageStars = !reader.IsDBNull(10) ? reader.GetDouble(10) : null,
                ComplimentHot = !reader.IsDBNull(11) ? reader.GetInt32(11) : null,
                ComplimentMore = !reader.IsDBNull(12) ? reader.GetInt32(12) : null,
                ComplimentProfile = !reader.IsDBNull(13) ? reader.GetInt32(13) : null,
                ComplimentCute = !reader.IsDBNull(14) ? reader.GetInt32(14) : null,
                ComplimentList = !reader.IsDBNull(15) ? reader.GetInt32(15) : null,
                ComplimentNote = !reader.IsDBNull(16) ? reader.GetInt32(16) : null,
                ComplimentPlain = !reader.IsDBNull(17) ? reader.GetInt32(17) : null,
                ComplimentCool = !reader.IsDBNull(18) ? reader.GetInt32(18) : null,
                ComplimentFunny = !reader.IsDBNull(19) ? reader.GetInt32(19) : null,
                ComplimentWriter = !reader.IsDBNull(20) ? reader.GetInt32(20) : null,
                ComplimentPhotos = !reader.IsDBNull(21) ? reader.GetInt32(21) : null
            });
        }

        return users;
    }

    public async Task<List<Checkin>> GetAllCheckinsAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"SELECT * FROM checkin";

        var checkins = new List<Checkin>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            checkins.Add(new Checkin
            {
                BusinessId = reader.GetString(0),
                Date = reader.GetString(1)
            });
        }

        return checkins;
    }

    public async Task<List<Tip>> GetAllTipsAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"SELECT * FROM tip";

        var tips = new List<Tip>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            tips.Add(new Tip
            {
                Text = reader.GetString(0),
                Date = reader.GetString(1),
                ComplimentCount = reader.GetInt32(2),
                BusinessId = reader.GetString(3),
                UserId = reader.GetString(4)
            });
        }

        return tips;
    }

    public async Task<List<Photo>> GetAllPhotosAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"SELECT * FROM photo";

        var photos = new List<Photo>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            photos.Add(new Photo
            {
                PhotoId = reader.GetString(0),
                BusinessId = reader.GetString(1),
                Caption = reader.GetString(2),
                Label = reader.GetString(3)
            });
        }

        return photos;
    }

    public async Task<List<Business>> SearchBusinessesByCategoryAndCityAsync(string category, string city)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT * FROM business
            WHERE categories LIKE @category
            AND city = @city";

        cmd.Parameters.AddWithValue("category", $"%{category}%");
        cmd.Parameters.AddWithValue("city", city);

        var businesses = new List<Business>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var business = new Business
            {
                BusinessId = reader.GetString(0),
                Name = reader.GetString(1),
                Address = !reader.IsDBNull(2) ? reader.GetString(2) : null,
                City = !reader.IsDBNull(3) ? reader.GetString(3) : null,
                State = !reader.IsDBNull(4) ? reader.GetString(4) : null,
                PostalCode = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                Latitude = !reader.IsDBNull(6) ? reader.GetDouble(6) : null,
                Longitude = !reader.IsDBNull(7) ? reader.GetDouble(7) : null,
                Stars = !reader.IsDBNull(8) ? reader.GetDouble(8) : null,
                ReviewCount = !reader.IsDBNull(9) ? reader.GetInt32(9) : null,
                IsOpen = !reader.IsDBNull(10) ? reader.GetInt32(10) : null,
                Categories = !reader.IsDBNull(12) ? reader.GetString(12) : null
            };

            // Handle attributes JSON
            if (!reader.IsDBNull(11))
            {
                string attributesJson = reader.GetString(11);
                business.RawAttributes = JsonDocument.Parse(attributesJson).RootElement;
            }

            // Handle hours
            if (!reader.IsDBNull(13))
            {
                string hoursJson = reader.GetString(13);
                business.Hours = JsonSerializer.Deserialize<Dictionary<string, string>>(hoursJson);
            }

            businesses.Add(business);
        }

        return businesses;
    }

    public async Task<List<Photo>> GetPhotosByBusinessIdAsync(string businessId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT photo_id, business_id, caption, label
            FROM photo
            WHERE business_id = @business_id";

        cmd.Parameters.AddWithValue("business_id", businessId);

        var photos = new List<Photo>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            photos.Add(new Photo
            {
                PhotoId = reader.GetString(0),
                BusinessId = reader.GetString(1),
                Caption = reader.GetString(2),
                Label = reader.GetString(3)
            });
        }

        return photos;
    }

    public async Task<List<Business>> SearchRestaurantsByNamePrefixAsync(string namePrefix, int limit = 10)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT * FROM business
            WHERE name LIKE @name_prefix 
            AND categories LIKE '%Restaurant%'
            LIMIT @limit";

        cmd.Parameters.AddWithValue("name_prefix", $"{namePrefix}%");
        cmd.Parameters.AddWithValue("limit", limit);

        var businesses = new List<Business>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var business = new Business
            {
                BusinessId = reader.GetString(0),
                Name = reader.GetString(1),
                Address = !reader.IsDBNull(2) ? reader.GetString(2) : null,
                City = !reader.IsDBNull(3) ? reader.GetString(3) : null,
                State = !reader.IsDBNull(4) ? reader.GetString(4) : null,
                PostalCode = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                Latitude = !reader.IsDBNull(6) ? reader.GetDouble(6) : null,
                Longitude = !reader.IsDBNull(7) ? reader.GetDouble(7) : null,
                Stars = !reader.IsDBNull(8) ? reader.GetDouble(8) : null,
                ReviewCount = !reader.IsDBNull(9) ? reader.GetInt32(9) : null,
                IsOpen = !reader.IsDBNull(10) ? reader.GetInt32(10) : null,
                Categories = !reader.IsDBNull(12) ? reader.GetString(12) : null
            };

            // Handle attributes JSON
            if (!reader.IsDBNull(11))
            {
                string attributesJson = reader.GetString(11);
                business.RawAttributes = JsonDocument.Parse(attributesJson).RootElement;
            }

            // Handle hours
            if (!reader.IsDBNull(13))
            {
                string hoursJson = reader.GetString(13);
                business.Hours = JsonSerializer.Deserialize<Dictionary<string, string>>(hoursJson);
            }

            businesses.Add(business);
        }

        return businesses;
    }

    public async Task<List<Review>> GetMostRecentReviewsByBusinessIdAsync(string businessId, int limit = 10)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT review_id, user_id, business_id, stars, date,
                   text, useful, funny, cool
            FROM review
            WHERE business_id = @business_id
            ORDER BY date DESC
            LIMIT @limit";

        cmd.Parameters.AddWithValue("business_id", businessId);
        cmd.Parameters.AddWithValue("limit", limit);

        var reviews = new List<Review>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            reviews.Add(new Review
            {
                ReviewId = reader.GetString(0),
                UserId = reader.GetString(1),
                BusinessId = reader.GetString(2),
                Stars = reader.GetDouble(3),
                Date = reader.GetString(4),
                Text = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                Useful = !reader.IsDBNull(6) ? reader.GetInt32(6) : null,
                Funny = !reader.IsDBNull(7) ? reader.GetInt32(7) : null,
                Cool = !reader.IsDBNull(8) ? reader.GetInt32(8) : null
            });
        }

        return reviews;
    }
    
    public async Task<List<User>> GetUsersByReviewIdsAsync(List<string> userIds)
    {
        if (userIds == null || userIds.Count == 0)
            return new List<User>();

        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        // Create a comma-separated list of quoted user IDs for the IN clause
        string userIdList = string.Join(",", userIds.Select(id => $"'{id}'"));

        cmd.CommandText = $@"
            SELECT user_id, name, review_count, yelping_since, friends,
                   useful, funny, cool, fans, elite, average_stars,
                   compliment_hot, compliment_more, compliment_profile,
                   compliment_cute, compliment_list, compliment_note,
                   compliment_plain, compliment_cool, compliment_funny,
                   compliment_writer, compliment_photos
            FROM users
            WHERE user_id IN ({userIdList})";

        var users = new List<User>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            users.Add(new User
            {
                UserId = reader.GetString(0),
                Name = reader.GetString(1),
                ReviewCount = !reader.IsDBNull(2) ? reader.GetInt32(2) : null,
                YelpingSince = !reader.IsDBNull(3) ? reader.GetString(3) : null,
                Friends = !reader.IsDBNull(4) ? reader.GetString(4) : null,
                Useful = !reader.IsDBNull(5) ? reader.GetInt32(5) : null,
                Funny = !reader.IsDBNull(6) ? reader.GetInt32(6) : null,
                Cool = !reader.IsDBNull(7) ? reader.GetInt32(7) : null,
                Fans = !reader.IsDBNull(8) ? reader.GetInt32(8) : null,
                Elite = !reader.IsDBNull(9) ? reader.GetString(9) : null,
                AverageStars = !reader.IsDBNull(10) ? reader.GetDouble(10) : null,
                ComplimentHot = !reader.IsDBNull(11) ? reader.GetInt32(11) : null,
                ComplimentMore = !reader.IsDBNull(12) ? reader.GetInt32(12) : null,
                ComplimentProfile = !reader.IsDBNull(13) ? reader.GetInt32(13) : null,
                ComplimentCute = !reader.IsDBNull(14) ? reader.GetInt32(14) : null,
                ComplimentList = !reader.IsDBNull(15) ? reader.GetInt32(15) : null,
                ComplimentNote = !reader.IsDBNull(16) ? reader.GetInt32(16) : null,
                ComplimentPlain = !reader.IsDBNull(17) ? reader.GetInt32(17) : null,
                ComplimentCool = !reader.IsDBNull(18) ? reader.GetInt32(18) : null,
                ComplimentFunny = !reader.IsDBNull(19) ? reader.GetInt32(19) : null,
                ComplimentWriter = !reader.IsDBNull(20) ? reader.GetInt32(20) : null,
                ComplimentPhotos = !reader.IsDBNull(21) ? reader.GetInt32(21) : null
            });
        }

        return users;
    }
    
    public async Task<List<Review>> GetReviewsByBusinessIdSortedByStarsAsync(string businessId, double targetStars = 3.0)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT review_id, user_id, business_id, stars, date,
                   text, useful, funny, cool
            FROM review
            WHERE business_id = @business_id
            ORDER BY ABS(stars - @target_stars) ASC";

        cmd.Parameters.AddWithValue("business_id", businessId);
        cmd.Parameters.AddWithValue("target_stars", targetStars);

        var reviews = new List<Review>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            reviews.Add(new Review
            {
                ReviewId = reader.GetString(0),
                UserId = reader.GetString(1),
                BusinessId = reader.GetString(2),
                Stars = reader.GetDouble(3),
                Date = reader.GetString(4),
                Text = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                Useful = !reader.IsDBNull(6) ? reader.GetInt32(6) : null,
                Funny = !reader.IsDBNull(7) ? reader.GetInt32(7) : null,
                Cool = !reader.IsDBNull(8) ? reader.GetInt32(8) : null
            });
        }

        return reviews;
    }
    
    public async Task<List<Review>> GetReviewsByBusinessIdAsync(string businessId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = @"
            SELECT review_id, user_id, business_id, stars, date,
                   text, useful, funny, cool
            FROM review
            WHERE business_id = @business_id";

        cmd.Parameters.AddWithValue("business_id", businessId);

        var reviews = new List<Review>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            reviews.Add(new Review
            {
                ReviewId = reader.GetString(0),
                UserId = reader.GetString(1),
                BusinessId = reader.GetString(2),
                Stars = reader.GetDouble(3),
                Date = reader.GetString(4),
                Text = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                Useful = !reader.IsDBNull(6) ? reader.GetInt32(6) : null,
                Funny = !reader.IsDBNull(7) ? reader.GetInt32(7) : null,
                Cool = !reader.IsDBNull(8) ? reader.GetInt32(8) : null
            });
        }

        return reviews;
    }

    public void Dispose()
    {
        _dataSource?.Dispose();
    }
}

#pragma warning restore CS8601, CS8603