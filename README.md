## Project Structure and Usage

The repository follows a consistent layout for yearly biomass mapping workflows.

```text
data/
├── raw/<year>/
├── interim/<year>/
├── processed/<year>/
├── reference/
outputs/
├── rasters/<year>/
└── mapping/<year>/
scripts/
src/ag_res/
logs/
```

## Running Scripts

All scripts take a `--year` argument.

```bash
python scripts/<script_name>.py --year 2024
```

## Path Handling

Paths are managed through `src/ag_res/paths.py` to keep file access consistent and portable.

```python
from ag_res import paths

# args.year comes from argparse in each script
input_csv = paths.raw(args.year) / f"masc_summary_{args.year}.csv"
output_tif = paths.rasters(args.year) / f"biomass_code_and_tonnes_{args.year}.tif"
```

## Creating a New Year Folder Set

Use the setup script to create yearly subfolders under data and outputs.

```bash
python scripts/setup_year_folders.py --year 2024
```