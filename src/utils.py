import pathlib
import yaml

import geopandas as gpd
import pandas as pd
from pymongo import MongoClient

def get_mongodb_connection_from_credential_file(credential_path: pathlib.Path) -> Dict:
    with open(credential_path) as cred_file:
        credentials = yaml.load(cred_file, Loader=yaml.FullLoader)
    assert "mongo_user" in credentials.keys()
    assert "mongo_password" in credentials.keys()
    if "mongo_port" in credentials.keys():
        port_num = credentials["mongo_port"]
    else:
        port_num = "27017"
    mongo_connection_str = f"mongodb://{credentials['mongo_user']}:{credentials['mongo_password']}@localhost:{port_num}/"
    return MongoClient(mongo_connection_str)


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