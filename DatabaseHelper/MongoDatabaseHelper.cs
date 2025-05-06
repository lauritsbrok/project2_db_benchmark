using MongoDB.Driver;
using project2_db_benchmark.Models;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace project2_db_benchmark.DatabaseHelper
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

        public MongoDatabaseHelper()
        {
            var connectionString = $"mongodb://{Globals.MANGO_DB_USERNAME}:{Globals.MANGO_DB_PASSWORD}@localhost:27017";

            _client = new MongoClient(connectionString);
            _database = _client.GetDatabase(Globals.MANGO_DB_NAME);

            // Get collections
            _businesses = _database.GetCollection<Business>("businesses");
            _reviews = _database.GetCollection<Review>("reviews");
            _users = _database.GetCollection<User>("users");
            _checkins = _database.GetCollection<Checkin>("checkins");
            _tips = _database.GetCollection<Tip>("tips");
            _photos = _database.GetCollection<Photo>("photos");

            // Setup indexes only if needed
            EnsureIndexesExist().Wait();
        }

        private async Task EnsureIndexesExist()
        {
            // Check if indexes exist by checking if there are any documents in the businesses collection
            var businessCount = await _businesses.CountDocumentsAsync(Builders<Business>.Filter.Empty);
            var reviewCount = await _reviews.CountDocumentsAsync(Builders<Review>.Filter.Empty);
            
            // If data already exists, we assume indexes have been created
            if (businessCount > 0 && reviewCount > 0)
            {
                Console.WriteLine("MongoDB collections already have data, skipping index creation.");
                return;
            }
            
            Console.WriteLine("Creating MongoDB indexes...");
            SetupIndexes();
            Console.WriteLine("MongoDB indexes created successfully.");
        }

        private void SetupIndexes()
        {
            // Business indexes
            var businessBuilder = Builders<Business>.IndexKeys;
            var businessIndexModels = new List<CreateIndexModel<Business>>
            {
                new CreateIndexModel<Business>(businessBuilder.Ascending(b => b.Categories)),
                new CreateIndexModel<Business>(businessBuilder.Ascending(b => b.City)),
                new CreateIndexModel<Business>(businessBuilder.Descending(b => b.Stars)),
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
            };
            _users.Indexes.CreateMany(userIndexModels);

            // Checkin indexes (time-series)
            var checkinBuilder = Builders<Checkin>.IndexKeys;
            var checkinIndexModels = new List<CreateIndexModel<Checkin>>
            {
                new CreateIndexModel<Checkin>(checkinBuilder.Ascending(c => c.BusinessId)),
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

        public async Task InsertBusinessAsync(Business business)
        {
            await _businesses.InsertOneAsync(business);
        }

        public async Task InsertReviewAsync(Review review)
        {
            await _reviews.InsertOneAsync(review);
        }

        public async Task InsertUserAsync(User user)
        {
            await _users.InsertOneAsync(user);
        }

        public async Task InsertCheckinAsync(Checkin checkin)
        {
            await _checkins.InsertOneAsync(checkin);
        }

        public async Task InsertTipAsync(Tip tip)
        {
            await _tips.InsertOneAsync(tip);
        }

        public async Task InsertPhotoAsync(Photo photo)
        {
            await _photos.InsertOneAsync(photo);
        }

        public void DeleteDatabase()
        {
            _client.DropDatabase(Globals.MANGO_DB_NAME);
        }

        public async Task<List<Tip>> GetTipsByBusinessIdAsync(string businessId)
        {
            var filter = Builders<Tip>.Filter.Eq(t => t.BusinessId, businessId);
            return await _tips.Find(filter).ToListAsync();
        }

        public async Task<Business> GetBusinessByIdAsync(string businessId)
        {
            var filter = Builders<Business>.Filter.Eq(b => b.BusinessId, businessId);
            return await _businesses.Find(filter).FirstOrDefaultAsync();
        }

        public async Task<List<Review>> GetReviewsByIdAsync(string reviewId)
        {
            var filter = Builders<Review>.Filter.Eq(r => r.ReviewId, reviewId);
            return await _reviews.Find(filter).ToListAsync();
        }

        public async Task<User> GetUserByIdAsync(string userId)
        {
            var filter = Builders<User>.Filter.Eq(u => u.UserId, userId);
            return await _users.Find(filter).FirstOrDefaultAsync();
        }

        public async Task<List<Checkin>> GetCheckinsByBusinessIdAsync(string businessId)
        {
            var filter = Builders<Checkin>.Filter.Eq(c => c.BusinessId, businessId);
            return await _checkins.Find(filter).ToListAsync();
        }

        public async Task<Photo> GetPhotoByIdAsync(string photoId)
        {
            var filter = Builders<Photo>.Filter.Eq(p => p.PhotoId, photoId);
            return await _photos.Find(filter).FirstOrDefaultAsync();
        }

        public async Task<List<Business>> SearchBusinessesByCategoryAndCityAsync(string category, string city)
        {
            var filter = Builders<Business>.Filter.And(
                Builders<Business>.Filter.Regex(b => b.Categories, new MongoDB.Bson.BsonRegularExpression(category, "i")),
                Builders<Business>.Filter.Eq(b => b.City, city)
            );
            return await _businesses.Find(filter).ToListAsync();
        }

        public async Task<List<Photo>> GetPhotosByBusinessIdAsync(string businessId)
        {
            var filter = Builders<Photo>.Filter.Eq(p => p.BusinessId, businessId);
            return await _photos.Find(filter).ToListAsync();
        }

        public async Task<List<Business>> GetAllBusinessesAsync()
        {
            return await _businesses.Find(_ => true).ToListAsync();
        }

        public async Task<List<Review>> GetAllReviewsAsync()
        {
            return await _reviews.Find(_ => true).ToListAsync();
        }

        public async Task<List<User>> GetAllUsersAsync()
        {
            return await _users.Find(_ => true).ToListAsync();
        }

        public async Task<List<Checkin>> GetAllCheckinsAsync()
        {
            return await _checkins.Find(_ => true).ToListAsync();
        }

        public async Task<List<Tip>> GetAllTipsAsync()
        {
            return await _tips.Find(_ => true).ToListAsync();
        }

        public async Task<List<Photo>> GetAllPhotosAsync()
        {
            return await _photos.Find(_ => true).ToListAsync();
        }

        public async Task<List<Business>> SearchBusinessesByNamePrefixAsync(string namePrefix, int limit = 10)
        {
            // Create a regex filter for the name prefix
            var regexPattern = $"^{namePrefix}";
            var filter = Builders<Business>.Filter.And(
                Builders<Business>.Filter.Regex(b => b.Name, new MongoDB.Bson.BsonRegularExpression(regexPattern, "i"))
            );

            return await _businesses.Find(filter).Limit(limit).ToListAsync();
        }

        public async Task<List<Review>> GetMostRecentReviewsByBusinessIdAsync(string businessId, int limit = 10)
        {
            var filter = Builders<Review>.Filter.Eq(r => r.BusinessId, businessId);
            return await _reviews.Find(filter)
                .Sort(Builders<Review>.Sort.Descending(r => r.Date))
                .Limit(limit)
                .ToListAsync();
        }

        public async Task<List<User>> GetUsersByReviewIdsAsync(List<string> userIds)
        {
            if (userIds == null || userIds.Count == 0)
                return new List<User>();

            var filter = Builders<User>.Filter.In(u => u.UserId, userIds);
            return await _users.Find(filter).ToListAsync();
        }

        public async Task<List<Review>> GetReviewsByBusinessIdSortedByStarsAsync(string businessId, double targetStars = 3.0)
        {
            var filter = Builders<Review>.Filter.Eq(r => r.BusinessId, businessId);
            
            // This is an approximation of ordering by absolute difference from target stars
            // MongoDB doesn't have a direct ABS function in the same way SQL does in sorting
            var reviews = await _reviews.Find(filter).ToListAsync();
            
            // Sort in memory by the absolute difference from target stars
            return reviews.OrderBy(r => Math.Abs(r.Stars - targetStars)).ToList();
        }

        public async Task<List<Review>> GetReviewsByBusinessIdAsync(string businessId)
        {
            var filter = Builders<Review>.Filter.Eq(r => r.BusinessId, businessId);
            return await _reviews.Find(filter).ToListAsync();
        }
    }
}