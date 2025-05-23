# Database Benchmark Project

This project compares the performance of MongoDB and PostgreSQL databases using the Yelp dataset.

## Structure

- `mongodb/` - MongoDB models and helpers
  - `Models/` - C# classes for MongoDB documents
  - `MongoDatabaseHelper.cs` - Helper class for MongoDB operations
- `postgres/` - PostgreSQL helpers
  - `PostgresDatabaseHelper.cs` - Helper class for PostgreSQL operations
- `Benchmarking/` - Performance comparison tools
  - `DatabaseBenchmark.cs` - Benchmarking utility for comparing MongoDB and PostgreSQL

## MongoDB Schema

The MongoDB implementation follows a document-oriented design with the following collections:

1. **businesses** - Stores business data with embedded address and location information
2. **reviews** - Stores review data with references to businesses and users
3. **users** - Stores user data with stats and friend relationships
4. **checkins** - Stores checkin data as a time series
5. **tips** - Stores tip data with text and references
6. **photos** - Stores photo metadata with references to businesses

## PostgreSQL Schema

The PostgreSQL implementation follows a relational model with tables corresponding to the main dataset entities.

## Running the Project

1. Start the MongoDB and PostgreSQL containers:

   ```
   docker-compose up -d
   ```

2. Run the project to perform benchmark tests:
   ```
   dotnet run --database <mongo or postgres> --num_concurrent <any number>
   ```

## Benchmarking

To run the benchmarking for write:

`./run_benchmarks.sh`

This will save to csv files in the results dir; mongo.csv and postgres.csv

To visualize it run:

`python plot_metrics.py postgres.csv mongo.csv postgres mongo`

The 5 resulting plots will be saved to the plots directory

## Check what is in postgres database

`psql -h localhost -p 5433 -U myuser -d postgres_yelp`

to see tables: `\dt`
To see what is inside tables: `SELECT * FROM businesses limit 5;`
