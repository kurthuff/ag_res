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
├── mapping/<year>/
└── reports/
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
Use on-page *Search Summary* for complete acres; `.xlsx` omits crop varieties
that are used in less than 500 acres or 3 farms. This is the cause of needing imputation, with *Search Summary* used to align hidden-valued instances.

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

## Pipeline Execution Order

The scripts below must be executed in this order to generate consistent ACI–MASC biomass outputs.
Each stage builds on the outputs of the previous one.

1. **setup_year_folders.py**  
   Initializes the folder structure for the specified year (raw, interim, processed, reports).

2. **aci_prepare.py**  
   Aggregates raw AAFC Annual Crop Inventory (ACI) raster data into municipality-level summaries.

3. **masc_impute.py**  
   Cleans and imputes missing yield and acreage data from MASC Excel sources.

4. **aci_reallocate_pixels.py**  
   Aligns ACI acreages with MASC totals at the RM × crop level.

5. **aci_muni_proportion.py**  
   Distributes reallocated RM-level acreages to individual municipalities using pixel share proportions.

6. **label_area_deltas.py**  
   Identifies over- and under-represented crops, reallocating surplus acreage from donor crops (“Other crops,” “Pasture/forages”, "Canola/rapeseed).

7. **aci_masc_merge.py**  
   Combines ACI and MASC data into a single table containing acreage, yield, biomass, and proportional weights. If biomass RPR-SAF is every updated,
   this script and any downstream must be re-run.

8. **aci_biomass_per_pixel.py**  
   Converts acreage and yield data to calculated biomass per-pixel values; imputes missing yields/biomasses and normalizes to MASC ground-truth totals.

9. **(Next phase)** – *biomass_from_yield.py* (planned)  
   Converts per-pixel yields to biomass using crop-specific Residue Production Factors (RPF) and Straw-to-Grain Ratios (SAF).