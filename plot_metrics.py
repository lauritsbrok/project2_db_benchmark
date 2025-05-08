# python plot_metrics.py mongo.csv postgres.csv MongoDB PostgreSQL
import pandas as pd
import matplotlib.pyplot as plt
import os
import sys

keys_to_col_name = {
    "totalTime(s)": "TotalTime (s)",
    "throughput(tuples/s)": "Throughput (tuples/s)",
    "avgLatency(ms)": "Average Latency (ms)",
    "99thPercentileLatency(ms)": "99th Percentile Latency (ms)",
    "90thPercentileLatency(ms)": "90th Percentile Latency (ms)",
}

output_name_to_title = {
    "write": "Concurrent write transactions",
    "user_read": "Concurrent read transactions",
    "user_story": "User story transactions",
}


def create_comparison_plot(df1, df2, x_col, y_col, output_name, name, label1, label2):
    plt.figure(figsize=(10, 6))

    # Plot first dataset in blue
    plt.plot(
        df1[x_col],
        df1[y_col],
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=8,
        color="blue",
        label=label1,
    )

    # Plot second dataset in red
    plt.plot(
        df2[x_col],
        df2[y_col],
        marker="s",
        linestyle="-",
        linewidth=2,
        markersize=8,
        color="red",
        label=label2,
    )

    plt.title(output_name_to_title[output_name])
    plt.xlabel("Number of concurrent requests")
    plt.ylabel(keys_to_col_name[y_col])
    plt.grid(True)
    plt.legend()

    # Save the plot
    output_path = os.path.join("plots_the_real_deal", f"{output_name}_{name}.png")
    plt.savefig(output_path)
    plt.close()


def main():
    if len(sys.argv) != 6:
        print(
            "Usage: python plot_metrics.py <input_csv1> <input_csv2> <label1> <label2> <output_name>"
        )
        print(
            "Example: python plot_metrics.py mongo.csv postgres.csv MongoDB PostgreSQL"
        )
        sys.exit(1)

    input_file1 = sys.argv[1]
    input_file2 = sys.argv[2]
    label1 = sys.argv[3]
    label2 = sys.argv[4]
    output_name = sys.argv[5]

    # Read the CSV files from the results directory
    input_path1 = input_file1
    input_path2 = input_file2

    if not os.path.exists(input_path1):
        print(f"Error: Input file {input_path1} not found!")
        sys.exit(1)
    if not os.path.exists(input_path2):
        print(f"Error: Input file {input_path2} not found!")
        sys.exit(1)

    # Create plots directory if it doesn't exist
    os.makedirs("plots_the_real_deal", exist_ok=True)

    # Read the CSV files
    df1 = pd.read_csv(input_path1)
    df2 = pd.read_csv(input_path2)

    # Create the five plots
    metrics_to_plot = [
        "totalTime(s)",
        "throughput(tuples/s)",
        "avgLatency(ms)",
        "99thPercentileLatency(ms)",
        "90thPercentileLatency(ms)",
    ]

    names = [
        "totalTime",
        "throughput",
        "avgLatency",
        "99thPercentileLatency",
        "90thPercentileLatency",
    ]

    for name, metric in zip(names, metrics_to_plot):
        create_comparison_plot(
            df1, df2, "numConcurrent", metric, output_name, name, label1, label2
        )

    print(
        f"Comparison plots have been saved in the plots directory with prefix {output_name}_"
    )


if __name__ == "__main__":
    main()
