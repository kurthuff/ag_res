from pathlib import Path

project_root = Path(__file__).resolve().parents[2]

def raw(year: int) -> Path:
    return project_root / "data" / "raw" / str(year)

def interim(year: int) -> Path:
    return project_root / "data" / "interim" / str(year)

def processed(year: int) -> Path:
    return project_root / "data" / "processed" / str(year)

def reference() -> Path:
    return project_root / "data" / "reference"

def rasters(year: int) -> Path:
    return project_root / "outputs" / "rasters" / str(year)

def mapping(year: int) -> Path:
    return project_root / "outputs" / "mapping" / str(year)

def logs() -> Path:
    return project_root / "logs"

def reports() -> Path:
    return project_root / "outputs" /  "reports"