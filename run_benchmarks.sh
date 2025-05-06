#!/bin/bash

# Create results directory if it doesn't exist
mkdir -p results

# Create/reset results files with headers
echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > results/postgres.csv
echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > results/mongo.csv

# Array of concurrency levels
concurrency_levels=(1 2 4 8 16 32 64 100)

# Function to run benchmarks for a specific database
run_benchmark() {
    local db=$1
    echo "Running benchmarks for $db..."

    for level in "${concurrency_levels[@]}"; do
        echo "Testing concurrency level: $level"

        # Restart containers to ensure clean state
        docker-compose down
        docker-compose up -d

        # Wait for databases to be ready
        sleep 10

        # Run the benchmark
        dotnet run --database "$db" --num_concurrent "$level"

        # Small pause between runs
        sleep 5
    done
}

# Run benchmarks for both databases
run_benchmark "postgres"
run_benchmark "mongo"

# Cleanup at the end
docker-compose down

echo "Benchmarking completed! Results are in results/postgres.csv and results/mongo.csv"