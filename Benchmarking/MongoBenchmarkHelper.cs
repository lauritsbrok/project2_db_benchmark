using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Helpers;
using project2_db_benchmark.Models.MongoDB;
using project2_db_benchmark.Models.Shared;

namespace project2_db_benchmark.Benchmarking
{
    public class MongoBenchmarkHelper()
    {
        private MongoDatabaseHelper _mongoHelper = new();
        private IEnumerable<Business> businesses = [];
        private IEnumerable<Checkin> checkins = [];
        private IEnumerable<Review> reviews = [];
        private IEnumerable<Tip> tips = [];
        private IEnumerable<User> users = [];

        public async Task<(double, double, List<double>)> LoadAndInsert(int concurrencyLevel = 8)
        {
            businesses = await Parser.Parse<Business>($"yelp_dataset/{Globals.BUSINESS_JSON_FILE_NAME}");
            IEnumerable<Photo> photos = await Parser.Parse<Photo>($"yelp_dataset/{Globals.PHOTO_JSON_FILE_NAME}");
            businesses = Business.AttachPhotosToBusinesses(businesses, photos);
            checkins = await Parser.Parse<Checkin>($"yelp_dataset/{Globals.CHECKIN_JSON_FILE_NAME}");
            reviews = await Parser.Parse<Review>($"yelp_dataset/{Globals.REVIEW_JSON_FILE_NAME}");
            tips = await Parser.Parse<Tip>($"yelp_dataset/{Globals.TIP_JSON_FILE_NAME}");
            users = await Parser.Parse<User>($"yelp_dataset/{Globals.USER_JSON_FILE_NAME}");

            var inserts = new List<Func<Task>>();

            inserts.AddRange(businesses.Select<Business, Func<Task>>(b => () => _mongoHelper.InsertBusinessAsync(b)));
            inserts.AddRange(checkins.Select<Checkin, Func<Task>>(c => () => _mongoHelper.InsertCheckinAsync(c)));
            inserts.AddRange(reviews.Select<Review, Func<Task>>(r => () => _mongoHelper.InsertReviewAsync(r)));
            inserts.AddRange(tips.Select<Tip, Func<Task>>(t => () => _mongoHelper.InsertTipAsync(t)));
            inserts.AddRange(users.Select<User, Func<Task>>(u => () => _mongoHelper.InsertUserAsync(u)));

            var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(inserts, concurrencyLevel);

            int totalRecords = businesses.Count() + checkins.Count() + reviews.Count() + tips.Count() + users.Count();
            double throughput = totalRecords / totalTime;

            return (totalTime, throughput, latencies);
        }

        public async Task<(double, double, List<double>)> BenchmarkReads(int concurrencyLevel = 8)
        {
            // Extract distinct business IDs for tip lookups
            var businesses = await Parser.Parse<Business>($"yelp_dataset/{Globals.BUSINESS_JSON_FILE_NAME}");
            var businessIds = businesses.Select(b => b.BusinessId).ToList();

            // Create a list of read operations
            var reads = new List<Func<Task>>();

            // Add tip lookups by business ID
            foreach (var business in businesses)
            {
                reads.Add(() => _mongoHelper.GetBusinessByIdAsync(business.BusinessId));
                reads.Add(() => _mongoHelper.GetCheckinsByBusinessIdAsync(business.BusinessId));
                reads.Add(() => _mongoHelper.GetTipsByBusinessIdAsync(business.BusinessId));
            }

            foreach (var user in users)
            {
                reads.Add(() => _mongoHelper.GetUserByIdAsync(user.UserId));
            }

            foreach (var review in reviews)
            {
                reads.Add(() => _mongoHelper.GetReviewsByIdAsync(review.ReviewId));
            }

            var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(reads, concurrencyLevel);

            int totalReads = reads.Count;
            double throughput = totalReads / totalTime;

            return (totalTime, throughput, latencies);
        }

        public async Task<(double, double, List<double>)> BenchmarkFullCollectionReads(int concurrencyLevel = 8)
        {
            var reads = new List<Func<Task>>();

            // Add operations to read all records from each collection
            reads.Add(() => _mongoHelper.GetAllBusinessesAsync());
            reads.Add(() => _mongoHelper.GetAllReviewsAsync());
            reads.Add(() => _mongoHelper.GetAllUsersAsync());
            reads.Add(() => _mongoHelper.GetAllCheckinsAsync());
            reads.Add(() => _mongoHelper.GetAllTipsAsync());

            var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(reads, concurrencyLevel);

            int totalReads = reads.Count;
            double throughput = totalReads / totalTime;

            return (totalTime, throughput, latencies);
        }
    }
}