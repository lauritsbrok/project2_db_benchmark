using MongoDB.Driver;
using project2_db_benchmark.Models.MongoDB;
using project2_db_benchmark.Models.Shared;

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

            // Setup indexes
            SetupIndexes();
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

        public void DeleteDatabase()
        {
            _client.DropDatabase(Globals.MANGO_DB_NAME);
        }
    }
} 