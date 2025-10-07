# scripts/setup_year_folders.py

from pathlib import Path
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=int)
    args = parser.parse_args()
    year = str(args.year)

    root = Path(__file__).resolve().parents[1]
    folders = [
        f"data/raw/{year}",
        f"data/interim/{year}",
        f"data/processed/{year}",
        f"outputs/rasters/{year}",
        f"outputs/mapping/{year}",
    ]

    for folder in folders:
        path = root / folder
        path.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    main()
