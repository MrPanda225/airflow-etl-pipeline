import csv
from pathlib import Path


class DataConsolidator:
    def __init__(self, staging_dir: Path):
        self.__staging = staging_dir

    # -------------------------------------------------------------------------
    # Final column order:
    # Rowid, Timestamp, Anon.Vehicle number, Vehicle type,
    # Number of axles, Tollplaza id, Tollplaza code,
    # Type of Payment code, Vehicle Code
    # -------------------------------------------------------------------------
    def consolidate(self) -> Path:
        csv_data   = self.__staging / "csv_data.csv"
        tsv_data   = self.__staging / "tsv_data.csv"
        fixed_data = self.__staging / "fixed_width_data.csv"
        dest       = self.__staging / "extracted_data.csv"

        with (
            open(csv_data,   newline="") as f_csv,
            open(tsv_data,   newline="") as f_tsv,
            open(fixed_data, newline="") as f_fixed,
            open(dest, "w",  newline="") as fout,
        ):
            csv_reader   = csv.reader(f_csv)
            tsv_reader   = csv.reader(f_tsv)
            fixed_reader = csv.reader(f_fixed)
            writer       = csv.writer(fout)

            for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
                writer.writerow([
                    *csv_row,    # Rowid, Timestamp, Anon.Vehicle number, Vehicle type
                    *tsv_row,    # Number of axles, Tollplaza id, Tollplaza code
                    *fixed_row,  # Type of Payment code, Vehicle Code
                ])

        return dest