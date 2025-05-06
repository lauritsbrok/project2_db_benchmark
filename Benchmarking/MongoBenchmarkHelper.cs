using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Helpers;
using project2_db_benchmark.Models;

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
        private IEnumerable<Photo> photos = [];

        public async Task<(double, double, List<double>)> LoadAndInsert(int concurrencyLevel = 8)
        {
            businesses = await Parser.Parse<Business>($"yelp_dataset/{Globals.BUSINESS_JSON_FILE_NAME}");
            checkins = await Parser.Parse<Checkin>($"yelp_dataset/{Globals.CHECKIN_JSON_FILE_NAME}");
            reviews = await Parser.Parse<Review>($"yelp_dataset/{Globals.REVIEW_JSON_FILE_NAME}");
            tips = await Parser.Parse<Tip>($"yelp_dataset/{Globals.TIP_JSON_FILE_NAME}");
            users = await Parser.Parse<User>($"yelp_dataset/{Globals.USER_JSON_FILE_NAME}");
            photos = await Parser.Parse<Photo>($"yelp_dataset/{Globals.PHOTO_JSON_FILE_NAME}");

            var inserts = new List<Func<Task>>();

            inserts.AddRange(businesses.Select<Business, Func<Task>>(b => () => _mongoHelper.InsertBusinessAsync(b)));
            inserts.AddRange(checkins.Select<Checkin, Func<Task>>(c => () => _mongoHelper.InsertCheckinAsync(c)));
            inserts.AddRange(reviews.Select<Review, Func<Task>>(r => () => _mongoHelper.InsertReviewAsync(r)));
            inserts.AddRange(tips.Select<Tip, Func<Task>>(t => () => _mongoHelper.InsertTipAsync(t)));
            inserts.AddRange(users.Select<User, Func<Task>>(u => () => _mongoHelper.InsertUserAsync(u)));
            inserts.AddRange(photos.Select<Photo, Func<Task>>(p => () => _mongoHelper.InsertPhotoAsync(p)));

            var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(inserts, concurrencyLevel);

            int totalRecords = businesses.Count() + checkins.Count() + reviews.Count() + tips.Count() + users.Count() + photos.Count();
            double throughput = totalRecords / totalTime;

            return (totalTime, throughput, latencies);
        }

        public async Task<(double, double, List<double>)> BenchmarkReads(int concurrencyLevel = 8)
        {
            businesses = await Parser.Parse<Business>($"yelp_dataset/{Globals.BUSINESS_JSON_FILE_NAME}");
            checkins = await Parser.Parse<Checkin>($"yelp_dataset/{Globals.CHECKIN_JSON_FILE_NAME}");
            reviews = await Parser.Parse<Review>($"yelp_dataset/{Globals.REVIEW_JSON_FILE_NAME}");
            tips = await Parser.Parse<Tip>($"yelp_dataset/{Globals.TIP_JSON_FILE_NAME}");
            users = await Parser.Parse<User>($"yelp_dataset/{Globals.USER_JSON_FILE_NAME}");
            photos = await Parser.Parse<Photo>($"yelp_dataset/{Globals.PHOTO_JSON_FILE_NAME}");

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

            foreach (var photo in photos)
            {
                reads.Add(() => _mongoHelper.GetPhotoByIdAsync(photo.PhotoId));
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
            reads.Add(() => _mongoHelper.GetAllPhotosAsync());

            var (totalTime, latencies) = await ConcurrentBenchmarkHelper.RunTasks(reads, concurrencyLevel);

            int totalReads = reads.Count;
            double throughput = totalReads / totalTime;

            return (totalTime, throughput, latencies);
        }
    }
}