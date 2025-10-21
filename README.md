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

## Data Sources

### MASC (Crop and Yield by RM)
https://www.masc.mb.ca/masc.nsf/mmpp_browser_variety.html

Annual yield and area by municipality/RM and crop (since 1994).  
Use on-page *Search Summary* for complete acres; `.xlsx` omits small cells.  
Scraper: `scripts/masc_scrape.py`.

### AAFC Annual Crop Inventory (ACI)
https://agriculture.canada.ca/atlas/data_donnees/annualCropInventory/data_donnees/tif/

Canada-wide land use raster (30 m), 2009–present.  
Lookup: `data/reference/aci_crop_classifications_iac_classifications_des_cultures.csv`.  
Files discovered as `data/raw/aci/aci_<year>_mb_v*.tif`.

### Municipality Boundaries (AgriMaps)
https://experience.arcgis.com/experience/90713dfec03b47d6a754628191cbeb4a/page/Page

Polygons used to align pixels to RM units.  
LUT: `data/reference/muni_rm_lut.csv`.  
Export GeoJSON via Layer List → References → Municipal Boundaries.

### RPR and SAF Lookup
Residue-to-product ratios and surplus availability factors.  
File: `data/reference/rpr_saf_masc_crop.csv`.

### Crop ↔ Label Mapping
Maps MASC `Crop` to ACI `Label`.  
File: `data/reference/crop_label_lut.csv`