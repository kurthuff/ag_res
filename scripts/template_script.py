import argparse
from ag_res import paths

parser = argparse.ArgumentParser()
parser.add_argument("--year", required=True, type=int)
args = parser.parse_args()
year = args.year

input_file = paths.raw(year) / f"example_input_{year}.csv"
output_file = paths.processed(year) / f"example_output_{year}.csv"

# Example placeholder logic
# print(input_file)
# print(output_file)

print(f"Processing for year {year}")
