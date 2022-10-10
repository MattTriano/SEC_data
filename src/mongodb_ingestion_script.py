import datetime as dt
import pathlib

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

from edgar_data import download_bulk_daily_index_companyfacts_from_edgar
from utils import get_mongodb_client_from_credential_file, get_project_root_dir


def setup_company_daily_index_archive_collection(client: MongoClient) -> None:
    sec_db = client["SEC_database"]
    sec_companies = sec_db["company"]
    company_daily_index = sec_companies["daily_index"]
    company_daily_index.create_index([("cik", ASCENDING)], unique=True)


def get_company_daily_index_archive_collection(client: MongoClient) -> Collection:
    company_daily_index = client.SEC_database.company.daily_index
    return company_daily_index


def record_invalid_company_file(invalid_company_file_name: str, data_dir: pathlib.Path) -> None:
    invalid_company_file_path = data_dir.joinpath(
        "archive", "bulk", "daily-index", "invalid_company_files.csv"
    )
    if not invalid_company_file_path.is_file():
        with open(invalid_company_file_path, "w") as f:
            f.write(f"invalid_company_name,time_of_ingestion_attempt\n")
    else:
        with open(invalid_company_file_path, "a") as f:
            time_now = dt.datetime.now().strftime("%Y_%m_%d__%H:%M:%S")
            f.write(f"{invalid_company_file_name},{time_now}\n")


def ingest_companyfacts_jsons_into_database(
    company_daily_index: Collection, data_dir: pathlib.Path, docs_per_insert: int = 100
) -> None:
    start_from = company_daily_index.count_documents({})
    print(f"{start_from} documents already in database; picking up from there.")
    number_inserted = 0
    company_facts_file_path = data_dir.joinpath(
        "archive", "bulk", "daily-index", "companyfacts.zip"
    )
    try:
        with zipfile.ZipFile(company_facts_file_path, "r") as zf:
            company_info = zf.infolist()
            company_file_list = zf.filelist
            company_name_list = zf.namelist()
            company_name_chunks = (
                company_name_list[i : i + docs_per_insert]
                for i in range(0, len(company_name_list[start_from:]), docs_per_insert)
            )
            for company_name_chunk in company_name_chunks:
                company_files = []
                company_ciks = []
                for company_name in company_name_chunk:
                    with zf.open(company_name, "r") as jf:
                        company_file = json.loads(jf.read().decode("utf-8"))
                        if "cik" in company_file.keys():
                            company_cik = str(company_file["cik"]).zfill(10)
                            company_file["cik"] = company_cik
                            company_files.append(company_file)
                            company_ciks.append(company_cik)
                        else:
                            print(f"Invalid company file found: {company_name}")
                            record_invalid_company_file(
                                invalid_company_name=company_name, data_dir=data_dir
                            )
                results = company_daily_index.find({"cik": {"$in": company_ciks}}, {"cik": 1})
                already_inserted_ciks = [result["cik"] for result in results]
                new_ciks = list(set(company_ciks) - set(already_inserted_ciks))
                company_files_to_insert = [cf for cf in company_files if cf["cik"] in new_ciks]
                number_new = len(company_files_to_insert)
                company_daily_index.insert_many(company_files_to_insert)
                number_inserted = number_inserted + number_new
                print(
                    f"Docs inserted: this chunk: {number_new: >3},"
                    + f" this session: {number_inserted: > 7},"
                    + f" total: {number_inserted+start_from: > 7}"
                )
    except Exception as err:
        print(f"Error: {err}")
        print(f"Last company file: {company_name}")


def main(project_root_dir: pathlib.Path):
    data_dir = project_root_dir.joinpath("data")
    download_bulk_daily_index_companyfacts_from_edgar(data_dir=data_dir)

    client = get_mongodb_client_from_credential_file(
        credential_path=project_root_dir.joinpath("secrets/credentials.yaml")
    )
    company_daily_index = get_company_daily_index_archive_collection(client=client)
    ingest_companyfacts_jsons_into_database(
        company_daily_index=company_daily_index, data_dir=data_dir, docs_per_insert=200
    )
    pass


if __name__ == "__main__":
    project_root_dir = get_project_root_dir(
        project_name="sec", a_file_path=pathlib.Path(__file__).resolve()
    )
    print(f"project_root_dir: {project_root_dir}")
    main(project_root_dir)
