#!/bin/bash

set -a
source .env
set +a

# Create results directory if it doesn't exist
timestamp=$(date +"%Y%m%d%H%M%S")
results_dir="results_$timestamp"
mkdir -p "$results_dir"

# Create/reset results files with headers
echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > "$results_dir/postgres_write.csv"
echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > "$results_dir/mongo_write.csv"

echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > "$results_dir/postgres_user_read.csv"
echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > "$results_dir/mongo_user_read.csv"

echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > "$results_dir/postgres_full_table_read.csv"
echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > "$results_dir/mongo_full_table_read.csv"

echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > "$results_dir/postgres_user_story.csv"
echo "numConcurrent,totalTime(s),throughput(tuples/s),minLatency(ms),maxLatency(ms),avgLatency(ms),99thPercentileLatency(ms),90thPercentileLatency(ms)" > "$results_dir/mongo_user_story.csv"

echo -e "Database, Concurrency, Size" > "$results_dir/db_sizes.csv"

# Array of concurrency levels
# concurrency_levels=(2 4 8 16 32 64)
concurrency_levels=(8)

# Function to run benchmarks for a specific database
run_benchmark() {
    local db=$1
    local benchmark_type=$2
    echo "Running benchmarks for $db..."

    for level in "${concurrency_levels[@]}"; do
        echo "Testing concurrency level: $level"

        # Restart containers to ensure clean state
        if [ "$benchmark_type" == "write" ]; then
            docker-compose down
            docker-compose up -d
        fi
        # Wait for databases to be ready
        sleep 10

        # Run the benchmark
        perf stat -o "perf_results_${db}_${level}_${benchmark_type}.txt" -d -a -e cycles,instructions,cache-references,cache-misses,branch-misses,context-switches,cpu-migrations,page-faults bin/Release/net9.0/linux-x64/publish/project2_db_benchmark --database "$db" --num_concurrent "$level" --benchmark_type "$benchmark_type" --results_dir "$results_dir"

        sleep 2

        # Get DB size
        if [ "$benchmark_type" == "write" ]; then
            if [ "$db" == "postgres" ]; then
                size=$(docker exec -i postgres_yelp psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT pg_size_pretty(pg_database_size('$POSTGRES_DB'));" | grep -E ' [0-9.]+ [a-zA-Z]B')
            elif [ "$db" == "mongo" ]; then
                size=$(docker exec mongo_yelp mongosh "$MONGO_DB_NAME" \
                    --username "$MONGO_INITDB_ROOT_USERNAME" \
                    --password "$MONGO_INITDB_ROOT_PASSWORD" \
                    --authenticationDatabase admin \
                    --quiet \
                    --eval "print((db.stats().storageSize / (1024 * 1024 * 1024)).toFixed(6) + ' GB')")
            fi
        fi

        echo -e "$db, $level, $size" >> "$results_dir/db_sizes.csv"

        # Small pause between runs
        sleep 2
    done
}

# Run benchmarks for both databases
run_benchmark "postgres" "write"
run_benchmark "postgres" "user-read"
run_benchmark "postgres" "full-table-read"
run_benchmark "postgres" "user-story"

run_benchmark "mongo" "write"
run_benchmark "mongo" "user-read"
run_benchmark "mongo" "full-table-read"
run_benchmark "mongo" "user-story"

# Cleanup at the end
docker-compose down

# Remove the instruction file if it exists
if [ -f "instruction-set.json" ]; then
    rm "instruction-set.json"
    echo "Deleted instruction-set.json"
fi

echo "Benchmarking completed!"