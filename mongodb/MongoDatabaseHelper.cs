using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GeoJsonObjectModel;
using project2_db_benchmark.mongodb.Models;
using System.Text.Json;

namespace project2_db_benchmark.mongodb
{
    public class MongoDatabaseHelper
    {
        private readonly IMongoClient _client;
        private readonly IMongoDatabase _database;

        // Collections
        private readonly IMongoCollection<Business> _businesses;
        private readonly IMongoCollection<Review> _reviews;
        private readonly IMongoCollection<User> _users;
        private readonly IMongoCollection<Checkin> _checkins;
        private readonly IMongoCollection<Tip> _tips;
        private readonly IMongoCollection<Photo> _photos;

        public MongoDatabaseHelper(string connectionString, string databaseName = "yelp")
        {
            _client = new MongoClient(connectionString);
            _database = _client.GetDatabase(databaseName);

            // Get collections
            _businesses = _database.GetCollection<Business>("businesses");
            _reviews = _database.GetCollection<Review>("reviews");
            _users = _database.GetCollection<User>("users");
            _checkins = _database.GetCollection<Checkin>("checkins");
            _tips = _database.GetCollection<Tip>("tips");
            _photos = _database.GetCollection<Photo>("photos");

            // Setup indexes
            SetupIndexes();
        }

        private void SetupIndexes()
        {
            // Business indexes
            var businessBuilder = Builders<Business>.IndexKeys;
            var businessIndexModels = new List<CreateIndexModel<Business>>
            {
                new CreateIndexModel<Business>(businessBuilder.Geo2DSphere(b => b.Location)),
                new CreateIndexModel<Business>(businessBuilder.Ascending(b => b.Categories)),
                new CreateIndexModel<Business>(businessBuilder.Ascending(b => b.Address.City)),
                new CreateIndexModel<Business>(businessBuilder.Descending(b => b.RatingAvg)),
                new CreateIndexModel<Business>(businessBuilder.Descending(b => b.RatingCount))
            };
            _businesses.Indexes.CreateMany(businessIndexModels);

            // Review indexes
            var reviewBuilder = Builders<Review>.IndexKeys;
            var reviewIndexModels = new List<CreateIndexModel<Review>>
            {
                new CreateIndexModel<Review>(reviewBuilder.Ascending(r => r.BusinessId)),
                new CreateIndexModel<Review>(reviewBuilder.Ascending(r => r.UserId)),
                new CreateIndexModel<Review>(reviewBuilder.Descending(r => r.Date))
            };
            _reviews.Indexes.CreateMany(reviewIndexModels);

            // User indexes
            var userBuilder = Builders<User>.IndexKeys;
            var userIndexModels = new List<CreateIndexModel<User>>
            {
                new CreateIndexModel<User>(userBuilder.Ascending(u => u.Name)),
                new CreateIndexModel<User>(userBuilder.Descending(u => u.LastActive))
            };
            _users.Indexes.CreateMany(userIndexModels);

            // Checkin indexes (time-series)
            var checkinBuilder = Builders<Checkin>.IndexKeys;
            var checkinIndexModels = new List<CreateIndexModel<Checkin>>
            {
                new CreateIndexModel<Checkin>(checkinBuilder.Ascending(c => c.BusinessId)),
                new CreateIndexModel<Checkin>(checkinBuilder.Descending(c => c.Ts))
            };
            _checkins.Indexes.CreateMany(checkinIndexModels);

            // Tip indexes
            var tipBuilder = Builders<Tip>.IndexKeys;
            var tipIndexModels = new List<CreateIndexModel<Tip>>
            {
                new CreateIndexModel<Tip>(tipBuilder.Ascending(t => t.BusinessId)),
                new CreateIndexModel<Tip>(tipBuilder.Ascending(t => t.UserId)),
                new CreateIndexModel<Tip>(tipBuilder.Descending(t => t.Date))
            };
            _tips.Indexes.CreateMany(tipIndexModels);

            // Photo indexes
            var photoBuilder = Builders<Photo>.IndexKeys;
            var photoIndexModels = new List<CreateIndexModel<Photo>>
            {
                new CreateIndexModel<Photo>(photoBuilder.Ascending(p => p.BusinessId)),
                new CreateIndexModel<Photo>(photoBuilder.Ascending(p => p.Label))
            };
            _photos.Indexes.CreateMany(photoIndexModels);
        }

        // Insert methods
        public async Task InsertBusinessAsync(Business business)
        {
            await _businesses.InsertOneAsync(business);
        }

        public async Task InsertReviewAsync(Review review)
        {
            await _reviews.InsertOneAsync(review);

            // Update denormalized counts in business
            var updateBuilder = Builders<Business>.Update
                .Inc(b => b.RatingCount, 1)
                .Set(b => b.UpdatedAt, DateTime.UtcNow);
            await _businesses.UpdateOneAsync(b => b.Id == review.BusinessId, updateBuilder);
        }

        public async Task InsertUserAsync(User user)
        {
            await _users.InsertOneAsync(user);
        }

        public async Task InsertCheckinAsync(Checkin checkin)
        {
            await _checkins.InsertOneAsync(checkin);

            // Update denormalized counts in business
            var updateBuilder = Builders<Business>.Update
                .Inc(b => b.CheckinCount, 1)
                .Set(b => b.UpdatedAt, DateTime.UtcNow);
            await _businesses.UpdateOneAsync(b => b.Id == checkin.BusinessId, updateBuilder);
        }

        public async Task InsertTipAsync(Tip tip)
        {
            await _tips.InsertOneAsync(tip);

            // Update denormalized counts in business
            var updateBuilder = Builders<Business>.Update
                .Inc(b => b.TipCount, 1)
                .Set(b => b.UpdatedAt, DateTime.UtcNow);
            await _businesses.UpdateOneAsync(b => b.Id == tip.BusinessId, updateBuilder);
        }

        public async Task InsertPhotoAsync(Photo photo)
        {
            await _photos.InsertOneAsync(photo);

            // Update denormalized counts in business
            var updateBuilder = Builders<Business>.Update
                .Inc(b => b.PhotoCount, 1)
                .Set(b => b.UpdatedAt, DateTime.UtcNow);
            await _businesses.UpdateOneAsync(b => b.Id == photo.BusinessId, updateBuilder);
        }

        // Bulk insert methods for importing data
        public async Task BulkInsertBusinessesAsync(IEnumerable<Business> businesses)
        {
            await _businesses.InsertManyAsync(businesses);
        }

        public async Task BulkInsertReviewsAsync(IEnumerable<Review> reviews)
        {
            await _reviews.InsertManyAsync(reviews);
        }

        // Import from JSON files
        public async Task ImportBusinessesFromFileAsync(string filePath, int batchSize = 1000)
        {
            await ImportJsonFileInBatchesAsync<Business>(filePath, _businesses, batchSize);
        }

        public async Task ImportReviewsFromFileAsync(string filePath, int batchSize = 1000)
        {
            await ImportJsonFileInBatchesAsync<Review>(filePath, _reviews, batchSize);
        }

        public async Task ImportUsersFromFileAsync(string filePath, int batchSize = 1000)
        {
            await ImportJsonFileInBatchesAsync<User>(filePath, _users, batchSize);
        }

        public async Task ImportTipsFromFileAsync(string filePath, int batchSize = 1000)
        {
            await ImportJsonFileInBatchesAsync<Tip>(filePath, _tips, batchSize);
        }

        public async Task ImportCheckinsFromFileAsync(string filePath, int batchSize = 1000)
        {
            await ImportJsonFileInBatchesAsync<Checkin>(filePath, _checkins, batchSize);
        }

        public async Task ImportPhotosFromFileAsync(string filePath, int batchSize = 1000)
        {
            await ImportJsonFileInBatchesAsync<Photo>(filePath, _photos, batchSize);
        }

        private async Task ImportJsonFileInBatchesAsync<T>(string filePath, IMongoCollection<T> collection, int batchSize)
        {
            int count = 0;
            List<T> batch = new List<T>(batchSize);

            using (StreamReader reader = new StreamReader(filePath))
            {
                string? line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    try
                    {
                        T? item = JsonSerializer.Deserialize<T>(line);
                        if (item != null)
                        {
                            batch.Add(item);
                            count++;

                            if (batch.Count >= batchSize)
                            {
                                await collection.InsertManyAsync(batch);
                                Console.WriteLine($"Inserted {count} items into {typeof(T).Name} collection");
                                batch.Clear();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error parsing line: {ex.Message}");
                    }
                }

                // Insert any remaining items
                if (batch.Count > 0)
                {
                    await collection.InsertManyAsync(batch);
                    Console.WriteLine($"Inserted {count} total items into {typeof(T).Name} collection");
                }
            }
        }

        // Query methods for benchmarking
        public async Task<List<Business>> FindBusinessesByRatingAsync(double minRating, int limit = 100)
        {
            var filter = Builders<Business>.Filter.Gt(b => b.RatingAvg, minRating);
            return await _businesses.Find(filter).Limit(limit).ToListAsync();
        }

        public async Task<List<Review>> FindReviewsByBusinessIdAsync(string businessId, int limit = 100)
        {
            var filter = Builders<Review>.Filter.Eq(r => r.BusinessId, businessId);
            return await _reviews.Find(filter).Limit(limit).ToListAsync();
        }

        public async Task<List<User>> FindUsersByNameAsync(string namePattern, int limit = 100)
        {
            var filter = Builders<User>.Filter.Regex(u => u.Name, new BsonRegularExpression(namePattern, "i"));
            return await _users.Find(filter).Limit(limit).ToListAsync();
        }

        public async Task<List<Business>> FindBusinessesByLocationAsync(double longitude, double latitude, double maxDistanceMeters, int limit = 100)
        {
            var point = new GeoJsonPoint<GeoJson2DGeographicCoordinates>(
                new GeoJson2DGeographicCoordinates(longitude, latitude)
            );
            
            var pointFilter = Builders<Business>.Filter.Near(
                b => b.Location,
                point,
                maxDistanceMeters
            );
            
            return await _businesses.Find(pointFilter).Limit(limit).ToListAsync();
        }

        public async Task<List<Checkin>> FindCheckinsByBusinessIdAsync(string businessId, DateTime startDate, DateTime endDate, int limit = 100)
        {
            var filter = Builders<Checkin>.Filter.And(
                Builders<Checkin>.Filter.Eq(c => c.BusinessId, businessId),
                Builders<Checkin>.Filter.Gte(c => c.Ts, startDate),
                Builders<Checkin>.Filter.Lte(c => c.Ts, endDate)
            );
            
            return await _checkins.Find(filter).Limit(limit).ToListAsync();
        }
    }
} 