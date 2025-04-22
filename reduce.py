import argparse
import json

def reduce_json_lines(input_file, output_file, max_entries=1000):
    with open(input_file, 'r', encoding='utf-8') as infile:
        lines = [json.loads(line) for _, line in zip(range(max_entries), infile)]

    with open(output_file, 'w', encoding='utf-8') as outfile:
        for entry in lines:
            json.dump(entry, outfile)
            outfile.write('\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reduce a JSON Lines file to a maximum number of entries.")
    parser.add_argument("input_file", help="Path to the input JSON Lines file")
    parser.add_argument("output_file", help="Path to save the reduced JSON Lines file")
    parser.add_argument("--max", type=int, default=1000, help="Maximum number of entries to keep (default: 1000)")

    args = parser.parse_args()
    reduce_json_lines(args.input_file, args.output_file, args.max)
