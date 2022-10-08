import pathlib

import geopandas as gpd
import pandas as pd

def extract_data_from_url(
    file_path: pathlib.Path,
    url: str,
    data_format: str,
    force_repull: bool = False,
) -> pd.DataFrame:
    file_path = pathlib.Path(file_path).resolve()
    file_path.parent.mkdir(exist_ok=True, parents=True)
    if not file_path.is_file() or force_repull:
        urlretrieve(url, file_path)
    if data_format in ["csv", "zipped_csv"]:
        return pd.read_csv(file_path)
    elif data_format in ["json"]:
        return pd.read_json(file_path)
    elif data_format in ["shp", "geojson"]:
        return gpd.read_file(file_path)