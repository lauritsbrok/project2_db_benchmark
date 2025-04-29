using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Helpers;
using project2_db_benchmark.Models.MongoDB;
using project2_db_benchmark.Models.Shared;

namespace project2_db_benchmark.Benchmarking
{
    public class MongoImportHelper()
    {
        private readonly MongoDatabaseHelper _mongoHelper = new();

        private async Task<double> LoadAndInsert(){
            IEnumerable<Business> businesses = await Parser.Parse<Business>("yelp_dataset/business_reduced.json");
            IEnumerable<Photo> photos = await Parser.Parse<Photo>("yelp_dataset/photo_reduced.json");
            businesses = Business.AttachPhotosToBusinesses(businesses, photos);
            IEnumerable<Checkin> checkins = await Parser.Parse<Checkin>("yelp_dataset/checkin_reduced.json");
            IEnumerable<Review> reviews = await Parser.Parse<Review>("yelp_dataset/review_reduced.json");
            IEnumerable<Tip> tips = await Parser.Parse<Tip>("yelp_dataset/tip_reduced.json");
            IEnumerable<User> users = await Parser.Parse<User>("yelp_dataset/user_reduced.json");

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


        public async Task<double> BenchmarkInsert(){
            return await LoadAndInsert();
        }
    }
} 