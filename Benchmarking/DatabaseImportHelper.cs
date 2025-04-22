using project2_db_benchmark.DatabaseHelper;
using project2_db_benchmark.Models.Shared;
using MongoBusiness = project2_db_benchmark.Models.MongoDB.Business;
using PostgresBusiness = project2_db_benchmark.Models.Postgres.Business;

namespace project2_db_benchmark.Benchmarking
{
    public class DatabaseImportHelper(MongoDatabaseHelper mongoHelper, PostgresDatabaseHelper postgresHelper)
    {
        private readonly MongoDatabaseHelper _mongoHelper = mongoHelper;
        private readonly PostgresDatabaseHelper _postgresHelper = postgresHelper;
        public async Task ImportAsync<T>(IEnumerable<T> records)
        {   
            foreach (var record in records)
            {
                switch (record)
                {
                    case MongoBusiness mongoBusiness:
                        await _mongoHelper.InsertBusinessAsync(mongoBusiness);
                        break;
                    case Review review:
                        await _mongoHelper.InsertReviewAsync(review);
                        break;
                    case User user:
                        await _mongoHelper.InsertUserAsync(user);
                        break;
                    case Checkin checkin:
                        await _mongoHelper.InsertCheckinAsync(checkin);
                        break;
                    case Tip tip:
                        await _mongoHelper.InsertTipAsync(tip);
                        break;
                    default:
                        throw new ArgumentException("Invalid import type");
                }
            }
        }
    }
} 