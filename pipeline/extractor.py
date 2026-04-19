# pipeline/extractor.py

import tarfile
import csv
import requests
from pathlib import Path
from urllib.parse import unquote


class DatasetDownloader:
    def __init__(self, url: str, dest: Path):
        self.__url = url
        self.__dest = dest

    def __extract_filename(self) -> str:
        return unquote(self.__url.split("/")[-1])

    def __prepare_destination(self):
        self.__dest.mkdir(parents=True, exist_ok=True)

    def download(self) -> Path:
        self.__prepare_destination()
        filepath = self.__dest / self.__extract_filename()

        with requests.get(self.__url, stream=True) as r:
            r.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        return filepath


class DatasetExtractor:
    def __init__(self, staging_dir: Path):
        self.__staging = staging_dir

    # -------------------------------------------------------------------------
    # Untar
    # -------------------------------------------------------------------------
    def untar(self, filepath: Path) -> None:
        with tarfile.open(filepath, "r:gz") as tar:
            tar.extractall(path=self.__staging)

    # -------------------------------------------------------------------------
    # Extract from CSV
    # vehicle-data.csv fields:
    # 0:Rowid | 1:Timestamp | 2:Anon.Vehicle number | 3:Vehicle type
    # 4:Number of axles | 5:Vehicle code
    # → keep: 0, 1, 2, 3
    # -------------------------------------------------------------------------
    def extract_from_csv(self) -> Path:
        src  = self.__staging / "vehicle-data.csv"
        dest = self.__staging / "csv_data.csv"

        with open(src, newline="") as fin, open(dest, "w", newline="") as fout:
            reader = csv.reader(fin)
            writer = csv.writer(fout)
            for row in reader:
                writer.writerow([row[0], row[1], row[2], row[3]])

        return dest

    # -------------------------------------------------------------------------
    # Extract from TSV
    # tollplaza-data.tsv fields:
    # 0:Rowid | 1:Timestamp | 2:Anon.Vehicle number | 3:Vehicle type
    # 4:Number of axles | 5:Tollplaza id | 6:Tollplaza code
    # → keep: 4, 5, 6
    # -------------------------------------------------------------------------
    def extract_from_tsv(self) -> Path:
        src  = self.__staging / "tollplaza-data.tsv"
        dest = self.__staging / "tsv_data.csv"

        with open(src, newline="") as fin, open(dest, "w", newline="") as fout:
            reader = csv.reader(fin, delimiter="\t")
            writer = csv.writer(fout)
            for row in reader:
                writer.writerow([row[4], row[5], row[6]])

        return dest

    # -------------------------------------------------------------------------
    # Extract from fixed-width
    # payment-data.txt fields (space-separated, timestamp spans 5 tokens):
    # 0:Rowid | 1-5:Timestamp | 6:Anon.Vehicle number | 7:Tollplaza id
    # 8:Tollplaza code | 9:Type of Payment code | 10:Vehicle Code
    # → keep: 9, 10
    # -------------------------------------------------------------------------
    def extract_from_fixed_width(self) -> Path:
        src  = self.__staging / "payment-data.txt"
        dest = self.__staging / "fixed_width_data.csv"

        with open(src) as fin, open(dest, "w", newline="") as fout:
            writer = csv.writer(fout)
            for line in fin:
                fields = line.strip().split()
                writer.writerow([fields[9], fields[10]])

        return dest