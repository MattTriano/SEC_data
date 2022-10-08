import os
import pathlib
from typing import Dict, List

import pandas as pd
import requests

from bs4 import BeautifulSoup
from bs4.element import ResultSet

def check_env_vars():
    print(os.environ["email"])
    return os.environ["email"]

def submit_request_to_sec(url) -> requests.models.Response:
    try:
        resp = requests.get(url, headers={"User-Agent": os.environ["email"]})
        if resp.status_code == 200:
            return resp
        else:
            raise Exception(
                f"Failed to get a valid response for url '{url}'\n"
                + f"Status code: {resp.status_code}. Is ENV VAR 'email' set?"
            )
    except Exception as err:
        print(f"Error: {err}; url: {url}")


def decode_lines(response_lines):
    decoded_lines = []
    error_lines = []
    for line in response_lines:
        try:
            decoded_lines.append(line.decode("utf-8"))
        except Exception:
            error_lines.append(line)
            print(line)
    return (decoded_lines, error_lines)


def get_edgar_cik_lookup_data(data_dir: pathlib.Path, force_repull: bool = False) -> pd.DataFrame:
    file_path = data_dir.joinpath("edgar-cik-lookup-data.csv")
    if not file_path.is_file() or force_repull:
        resp = requests.get(
            url="https://www.sec.gov/Archives/edgar/cik-lookup-data.txt",
            headers={"User-Agent": os.environ["email"]},
        )
        if resp.status_code == 200:
            response_lines = [line for line in resp.iter_lines()]
            decoded_lines, error_lines = decode_lines(response_lines)
            company_cik_codes = pd.DataFrame(decoded_lines)
            assert all(
                company_cik_codes[0].str[-11:-8] == "000"
            ), "A company had either a cik id >= 10^7 or a malformed cik. Investigate."
            company_cik_codes_df = pd.DataFrame(
                {"company": company_cik_codes[0].str[:-12], "cik": company_cik_codes[0].str[-11:-1]}
            )
            company_cik_codes_df.to_csv(file_path, index=False)
        else:
            raise Exception(f"Failed to get a valid response: {resp.status_code}")
    return pd.read_csv(file_path, dtype=str)


def extract_values_from_table_row(table_row_data: ResultSet, table_header: List) -> Dict:
    try:
        return {
            "rel_url": table_row_data[0].find("a", href=True)["href"],
            table_header[0]: table_row_data[0].text,
            table_header[1]: table_row_data[1].text.replace("\xa0", " "),
            table_header[2]: table_row_data[2].text,
        }
    except Exception as err:
        print(f"Error: {err}, table_data_row: {table_row_data}")


def extract_year_table_rows_from_edgar_archive_feed() -> ResultSet:
    base_url = "https://www.sec.gov/Archives/edgar/Feed/"
    resp = submit_request_to_sec(url=base_url)
    year_table_rows = extract_table_rows_from_response(resp=resp)
    return year_table_rows


def extract_year_qtr_table_rows_from_edgar_archive_feed(year_url: str) -> ResultSet:
    resp = submit_request_to_sec(url=year_url)
    qtr_table_rows = extract_table_rows_from_response(resp=resp)
    return qtr_table_rows


def extract_data_from_edgar_table_rows(table_rows: ResultSet) -> pd.DataFrame:
    table_header_line = table_rows[0].find_all("th")
    table_header = [el.text for el in table_header_line]
    data_rows = []
    for table_row in table_rows[1:]:
        table_row_data = table_row.find_all("td")
        data_rows.append(
            extract_values_from_table_row(table_row_data=table_row_data, table_header=table_header)
        )
    page_df = pd.DataFrame(data_rows)
    return page_df


def get_egdar_archive_years(data_dir: pathlib.Path, check_page: bool = False) -> pd.DataFrame:
    file_path = data_dir.joinpath("edgar_archive_years.csv")
    if not file_path.is_file() or check_page:
        year_table_rows = extract_year_table_rows_from_edgar_archive_feed()
        edgar_archive_years_df = extract_data_from_edgar_table_rows(table_rows=year_table_rows)
        edgar_archive_years_df["year_url"] = (
            "https://www.sec.gov/Archives/edgar/Feed/" + edgar_archive_years_df["rel_url"]
        )
        edgar_archive_years_df["Last Modified"] = pd.to_datetime(
            edgar_archive_years_df["Last Modified"]
        )
        edgar_archive_years_df.to_csv(file_path, index=False)
    edgar_archive_years_df = pd.read_csv(file_path, dtype=str)
    return edgar_archive_years_df


def extract_table_rows_from_response(
    resp: requests.models.Response, tables_expected: int = 1
) -> List:
    soup = BeautifulSoup(resp.content, "html.parser")
    tables = soup.find_all("table")
    assert (
        len(tables) == tables_expected
    ), f"More tables than expected found on the page at {resp.url}. Investigate."
    table_rows = tables[0].find_all("tr")
    return table_rows


def get_edgar_archive_year_quarters(
    year: str, data_dir: pathlib.Path, check_page: bool = False
) -> pd.DataFrame:
    archive_year_dir = data_dir.joinpath("archive", year)
    archive_year_dir.mkdir(exist_ok=True, parents=True)
    file_path = archive_year_dir.joinpath(f"edgar_qtr_data_for_{year}.csv")
    if not file_path.is_file() or check_page:
        year_url = f"https://www.sec.gov/Archives/edgar/Feed/{year}"
        resp = submit_request_to_sec(url=year_url)
        qtr_table_rows = extract_table_rows_from_response(resp=resp)
        edgar_archive_year_qtrs_df = extract_data_from_edgar_table_rows(table_rows=qtr_table_rows)
        edgar_archive_year_qtrs_df["year"] = year
        edgar_archive_year_qtrs_df.to_csv(file_path, index=False)
    edgar_archive_year_qtrs_df = pd.read_csv(file_path, dtype=str)
    return edgar_archive_year_qtrs_df
