using System;
using System.Threading.Tasks;
using project2_db_benchmark;

namespace project2_db_benchmark
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("PostgreSQL Test Tool");
            Console.WriteLine("===================");

            string yelpDatasetPath = "./yelp_dataset";

            // Create and run the PostgreSQL test
            var postgresTest = new PostgresTest(yelpDatasetPath);
            await postgresTest.RunPostgresTestAsync();

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }
}