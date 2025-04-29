using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Helpers;
using project2_db_benchmark.Models.Postgres;
using project2_db_benchmark.Models.Shared;

namespace project2_db_benchmark.Benchmarking
{
    public class PostgresImportHelper()
    {
        private readonly PostgresDatabaseHelper _postgresHelper = new();
        public async Task ImportJsonFiles(){
            IEnumerable<Business> businesses = await Parser.Parse<Business>("yelp_dataset/business_reduced.json");
            IEnumerable<Checkin> checkins = await Parser.Parse<Checkin>("yelp_dataset/checkin_reduced.json");
            IEnumerable<Review> reviews = await Parser.Parse<Review>("yelp_dataset/review_reduced.json");
            IEnumerable<Tip> tips = await Parser.Parse<Tip>("yelp_dataset/tip_reduced.json");
            IEnumerable<User> users = await Parser.Parse<User>("yelp_dataset/user_reduced.json");
            IEnumerable<Photo> photos = await Parser.Parse<Photo>("yelp_dataset/photo_reduced.json");

            foreach (var business in businesses)
                await _postgresHelper.InsertBusinessAsync(business);
            foreach (var checkin in checkins)
                await _postgresHelper.InsertCheckinAsync(checkin);
            foreach (var review in reviews)
                await _postgresHelper.InsertReviewAsync(review);
            foreach (var tip in tips)
                await _postgresHelper.InsertTipAsync(tip);
            foreach (var user in users)
                await _postgresHelper.InsertUserAsync(user);
            foreach (var photo in photos)
                await _postgresHelper.InsertPhotoAsync(photo);
        }
    }
} 