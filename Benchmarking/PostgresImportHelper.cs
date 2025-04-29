using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Helpers;
using project2_db_benchmark.Models.Postgres;
using project2_db_benchmark.Models.Shared;



namespace project2_db_benchmark.Benchmarking
{
    public class PostgresImportHelper()
    {
        private readonly PostgresDatabaseHelper _postgresHelper = new();
        public async Task<(double, double, List<double>)> LoadAndInsert()
        {
            IEnumerable<Business> businesses = await Parser.Parse<Business>($"yelp_dataset/{Globals.BUSINESS_JSON_FILE_NAME}");
            IEnumerable<Checkin> checkins = await Parser.Parse<Checkin>($"yelp_dataset/{Globals.CHECKIN_JSON_FILE_NAME}");
            IEnumerable<Review> reviews = await Parser.Parse<Review>($"yelp_dataset/{Globals.REVIEW_JSON_FILE_NAME}");
            IEnumerable<Tip> tips = await Parser.Parse<Tip>($"yelp_dataset/{Globals.TIP_JSON_FILE_NAME}");
            IEnumerable<User> users = await Parser.Parse<User>($"yelp_dataset/{Globals.USER_JSON_FILE_NAME}");
            IEnumerable<Photo> photos = await Parser.Parse<Photo>($"yelp_dataset/{Globals.PHOTO_JSON_FILE_NAME}");

            var inserts = new List<Func<Task>>();

            inserts.AddRange(businesses.Select<Business, Func<Task>>(b => () => _postgresHelper.InsertBusinessAsync(b)));
            inserts.AddRange(checkins.Select<Checkin, Func<Task>>(c => () => _postgresHelper.InsertCheckinAsync(c)));
            inserts.AddRange(reviews.Select<Review, Func<Task>>(r => () => _postgresHelper.InsertReviewAsync(r)));
            inserts.AddRange(tips.Select<Tip, Func<Task>>(t => () => _postgresHelper.InsertTipAsync(t)));
            inserts.AddRange(users.Select<User, Func<Task>>(u => () => _postgresHelper.InsertUserAsync(u)));
            inserts.AddRange(photos.Select<Photo, Func<Task>>(u => () => _postgresHelper.InsertPhotoAsync(u)));

            var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(inserts);

            int totalRecords = businesses.Count() + checkins.Count() + reviews.Count() + tips.Count() + users.Count();
            double throughput = totalRecords / totalTime;

            return (totalTime, throughput, latencies); ;
        }
    }
}