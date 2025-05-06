using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Helpers;
using project2_db_benchmark.Models.Postgres;
using project2_db_benchmark.Models.Shared;



namespace project2_db_benchmark.Benchmarking
{
    public class PostgresImportHelper()
    {
        private readonly PostgresDatabaseHelper _postgresHelper = new();
        private IEnumerable<Business> businesses = [];
        private IEnumerable<Checkin> checkins = [];
        private IEnumerable<Review> reviews = [];
        private IEnumerable<Tip> tips = [];
        private IEnumerable<User> users = [];
        private IEnumerable<Photo> photos = [];

        public async Task<(double, double, List<double>)> LoadAndInsert()
        {
            businesses = await Parser.Parse<Business>($"yelp_dataset/{Globals.BUSINESS_JSON_FILE_NAME}");
            checkins = await Parser.Parse<Checkin>($"yelp_dataset/{Globals.CHECKIN_JSON_FILE_NAME}");
            reviews = await Parser.Parse<Review>($"yelp_dataset/{Globals.REVIEW_JSON_FILE_NAME}");
            tips = await Parser.Parse<Tip>($"yelp_dataset/{Globals.TIP_JSON_FILE_NAME}");
            users = await Parser.Parse<User>($"yelp_dataset/{Globals.USER_JSON_FILE_NAME}");
            photos = await Parser.Parse<Photo>($"yelp_dataset/{Globals.PHOTO_JSON_FILE_NAME}");

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

        public async Task<(double, double, List<double>)> BenchmarkReads()
        {
            var reads = new List<Func<Task>>();

            foreach (var business in businesses)
            {
                reads.Add(() => _postgresHelper.GetBusinessByIdAsync(business.BusinessId));
                reads.Add(() => _postgresHelper.GetCheckinsByBusinessIdAsync(business.BusinessId));
                reads.Add(() => _postgresHelper.GetTipsByBusinessIdAsync(business.BusinessId));
            }

            foreach (var review in reviews)
            {
                reads.Add(() => _postgresHelper.GetReviewsByIdAsync(review.ReviewId));
            }

            foreach (var user in users)
            {
                reads.Add(() => _postgresHelper.GetUserByIdAsync(user.UserId));
            }

            foreach (var photo in photos)
            {
                reads.Add(() => _postgresHelper.GetPhotoByIdAsync(photo.PhotoId));
            }

            // Run the benchmarks
            var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(reads);

            int totalReads = reads.Count;
            double throughput = totalReads / totalTime;

            return (totalTime, throughput, latencies);
        }

        public async Task<(double, double, List<double>)> BenchmarkFullTableReads()
        {
            var reads = new List<Func<Task>>();

            // Add operations to read all records from each table
            reads.Add(() => _postgresHelper.GetAllBusinessesAsync());
            reads.Add(() => _postgresHelper.GetAllReviewsAsync());
            reads.Add(() => _postgresHelper.GetAllUsersAsync());
            reads.Add(() => _postgresHelper.GetAllCheckinsAsync());
            reads.Add(() => _postgresHelper.GetAllTipsAsync());
            reads.Add(() => _postgresHelper.GetAllPhotosAsync());

            var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(reads);

            int totalReads = reads.Count;
            double throughput = totalReads / totalTime;

            return (totalTime, throughput, latencies);
        }
    }
}