import matplotlib.pyplot as plt
import pandas as pd

# File paths
mongo_file = "/Users/sshb/Desktop/itu/2. semester CS/Computer system performance/project2_db_benchmark/results_the_real_deal/mongo_full_table_read.csv"
postgres_file = "/Users/sshb/Desktop/itu/2. semester CS/Computer system performance/project2_db_benchmark/results_the_real_deal/postgres_full_table_read.csv"

# Load CSV files into DataFrames
mongo_data = pd.read_csv(mongo_file)
postgres_data = pd.read_csv(postgres_file)

# Extract relevant data
databases = ["MongoDB", "PostgreSQL"]
total_time = [mongo_data["totalTime(s)"][0], postgres_data["totalTime(s)"][0]]


# Plot bar chart for throughput
plt.figure(figsize=(10, 5))
plt.bar(databases, total_time, color=["blue", "red"])
plt.title("Read time for all tables")
plt.ylabel("Total time (s)")
plt.xlabel("System")
# Save the plot as an image
output_path = "/Users/sshb/Desktop/itu/2. semester CS/Computer system performance/project2_db_benchmark/plots_the_real_deal/full_table.png"
plt.savefig(output_path, format="png", dpi=300)
