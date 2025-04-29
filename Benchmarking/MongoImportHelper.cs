using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Helpers;
using project2_db_benchmark.Models.MongoDB;
using project2_db_benchmark.Models.Shared;

namespace project2_db_benchmark.Benchmarking
{
    public class MongoImportHelper()
    {
        private readonly MongoDatabaseHelper _mongoHelper = new();

        public async Task<double> LoadAndInsert()
        {
            IEnumerable<Business> businesses = await Parser.Parse<Business>($"yelp_dataset/{Globals.BUSINESS_JSON_FILE_NAME}");
            IEnumerable<Photo> photos = await Parser.Parse<Photo>($"yelp_dataset/{Globals.PHOTO_JSON_FILE_NAME}");
            businesses = Business.AttachPhotosToBusinesses(businesses, photos);
            IEnumerable<Checkin> checkins = await Parser.Parse<Checkin>($"yelp_dataset/{Globals.CHECKIN_JSON_FILE_NAME}");
            IEnumerable<Review> reviews = await Parser.Parse<Review>($"yelp_dataset/{Globals.REVIEW_JSON_FILE_NAME}");
            IEnumerable<Tip> tips = await Parser.Parse<Tip>($"yelp_dataset/{Globals.TIP_JSON_FILE_NAME}");
            IEnumerable<User> users = await Parser.Parse<User>($"yelp_dataset/{Globals.TIP_JSON_FILE_NAME}");

            var inserts = new List<Func<Task>>();

            inserts.AddRange(businesses.Select<Business, Func<Task>>(b => () => _mongoHelper.InsertBusinessAsync(b)));
            inserts.AddRange(checkins.Select<Checkin, Func<Task>>(c => () => _mongoHelper.InsertCheckinAsync(c)));
            inserts.AddRange(reviews.Select<Review, Func<Task>>(r => () => _mongoHelper.InsertReviewAsync(r)));
            inserts.AddRange(tips.Select<Tip, Func<Task>>(t => () => _mongoHelper.InsertTipAsync(t)));
            inserts.AddRange(users.Select<User, Func<Task>>(u => () => _mongoHelper.InsertUserAsync(u)));

            var result = await ConcurrentBenchmarkHelper.RunTasks(inserts);

            _mongoHelper.DeleteDatabase();

            return result;
        }
    }
}