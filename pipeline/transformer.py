# pipeline/transformer.py

import csv
from pathlib import Path


class DataTransformer:
    def __init__(self, staging_dir: Path):
        self.__staging = staging_dir

    # -------------------------------------------------------------------------
    # Transform vehicle_type (col index 3) to uppercase
    # extracted_data.csv → transformed_data.csv
    # -------------------------------------------------------------------------
    def transform(self) -> Path:
        src  = self.__staging / "extracted_data.csv"
        dest = self.__staging / "transformed_data.csv"

        with open(src, newline="") as fin, open(dest, "w", newline="") as fout:
            reader = csv.reader(fin)
            writer = csv.writer(fout)
            for row in reader:
                row[3] = row[3].upper()
                writer.writerow(row)

        return dest