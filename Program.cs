// using System;
// using System.Threading.Tasks;
// using project2_db_benchmark;

// namespace project2_db_benchmark
// {
//     class Program
//     {
//         static async Task Main(string[] args)
//         {
//             Console.WriteLine("Database Benchmark Tool");
//             Console.WriteLine("=======================");

//             // Get MongoDB connection string
//             Console.Write("Enter MongoDB connection string (default: mongodb://localhost:27017): ");
//             string mongoConnectionString = Console.ReadLine();
//             if (string.IsNullOrWhiteSpace(mongoConnectionString))
//             {
//                 mongoConnectionString = "mongodb://localhost:27017";
//             }

//             // Get Yelp dataset path
//             Console.Write("Enter path to Yelp dataset folder (default: ./yelp_dataset): ");
//             string yelpDatasetPath = Console.ReadLine();
//             if (string.IsNullOrWhiteSpace(yelpDatasetPath))
//             {
//                 yelpDatasetPath = "./yelp_dataset";
//             }

//             // Create and run the benchmark
//             var benchmark = new DatabaseBenchmark(mongoConnectionString, yelpDatasetPath);
//             await benchmark.RunBenchmarkAsync();

//             Console.WriteLine("Press any key to exit...");
//             Console.ReadKey();
//         }
//     }
// }