# tests/test_pipeline.py

import csv
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from pipeline.extractor import DatasetDownloader, DatasetExtractor
from pipeline.consolidator import DataConsolidator
from pipeline.transformer import DataTransformer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def staging(tmp_path) -> Path:
    """Crée un répertoire staging temporaire propre pour chaque test."""
    return tmp_path / "staging"


@pytest.fixture
def populated_staging(staging) -> Path:
    """Staging pré-rempli avec des fichiers de test réalistes."""
    staging.mkdir()

    (staging / "vehicle-data.csv").write_text(
        "1,Thu Aug 19 21:54:38 2021,174434,four-wheeler,4,VC965\n"
        "2,Sat Jul 31 04:09:44 2021,8538286,six-wheeler,6,VC965\n"
    )
    (staging / "tollplaza-data.tsv").write_text(
        "1\tThu Aug 19 21:54:38 2021\t174434\tfour-wheeler\t4\t4856\tPC7C042B7\n"
        "2\tSat Jul 31 04:09:44 2021\t8538286\tsix-wheeler\t6\t4154\tPC2C2EF9E\n"
    )
    (staging / "payment-data.txt").write_text(
        "1 Thu Aug 19 21:54:38 2021 174434 4856 PC7C042B7 PTE VC965\n"
        "2 Sat Jul 31 04:09:44 2021 8538286 4154 PC2C2EF9E PTP VC965\n"
    )
    (staging / "csv_data.csv").write_text(
        "1,Thu Aug 19 21:54:38 2021,174434,four-wheeler\n"
        "2,Sat Jul 31 04:09:44 2021,8538286,six-wheeler\n"
    )
    (staging / "tsv_data.csv").write_text(
        "4,4856,PC7C042B7\n"
        "6,4154,PC2C2EF9E\n"
    )
    (staging / "fixed_width_data.csv").write_text(
        "PTE,VC965\n"
        "PTP,VC965\n"
    )
    (staging / "extracted_data.csv").write_text(
        "1,Thu Aug 19 21:54:38 2021,174434,four-wheeler,4,4856,PC7C042B7,PTE,VC965\n"
        "2,Sat Jul 31 04:09:44 2021,8538286,six-wheeler,6,4154,PC2C2EF9E,PTP,VC965\n"
    )

    return staging


# ---------------------------------------------------------------------------
# DatasetDownloader
# ---------------------------------------------------------------------------
class TestDatasetDownloader:

    def test_creates_staging_dir(self, staging):
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"fake data"]
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("pipeline.extractor.requests.get", return_value=mock_response):
            DatasetDownloader(url="http://fake.com/tolldata.tgz", dest=staging).download()

        assert staging.exists()

    def test_filename_extracted_from_url(self, staging):
        staging.mkdir()
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"fake data"]
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("pipeline.extractor.requests.get", return_value=mock_response):
            DatasetDownloader(url="http://fake.com/tolldata.tgz", dest=staging).download()

        assert (staging / "tolldata.tgz").exists()

    def test_url_encoded_filename_decoded(self, staging):
        staging.mkdir()
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"fake data"]
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("pipeline.extractor.requests.get", return_value=mock_response):
            DatasetDownloader(url="http://fake.com/toll%20data.tgz", dest=staging).download()

        assert (staging / "toll data.tgz").exists()


# ---------------------------------------------------------------------------
# DatasetExtractor
# ---------------------------------------------------------------------------
class TestDatasetExtractor:

    def test_extract_from_csv_creates_file(self, populated_staging):
        DatasetExtractor(populated_staging).extract_from_csv()
        assert (populated_staging / "csv_data.csv").exists()

    def test_extract_from_csv_correct_columns(self, populated_staging):
        DatasetExtractor(populated_staging).extract_from_csv()
        rows = list(csv.reader((populated_staging / "csv_data.csv").open()))
        assert rows[0] == ["1", "Thu Aug 19 21:54:38 2021", "174434", "four-wheeler"]

    def test_extract_from_tsv_creates_file(self, populated_staging):
        DatasetExtractor(populated_staging).extract_from_tsv()
        assert (populated_staging / "tsv_data.csv").exists()

    def test_extract_from_tsv_correct_columns(self, populated_staging):
        DatasetExtractor(populated_staging).extract_from_tsv()
        rows = list(csv.reader((populated_staging / "tsv_data.csv").open()))
        assert rows[0] == ["4", "4856", "PC7C042B7"]

    def test_extract_from_fixed_width_creates_file(self, populated_staging):
        DatasetExtractor(populated_staging).extract_from_fixed_width()
        assert (populated_staging / "fixed_width_data.csv").exists()

    def test_extract_from_fixed_width_correct_columns(self, populated_staging):
        DatasetExtractor(populated_staging).extract_from_fixed_width()
        rows = list(csv.reader((populated_staging / "fixed_width_data.csv").open()))
        assert rows[0] == ["PTE", "VC965"]


# ---------------------------------------------------------------------------
# DataConsolidator
# ---------------------------------------------------------------------------
class TestDataConsolidator:

    def test_consolidate_creates_file(self, populated_staging):
        DataConsolidator(populated_staging).consolidate()
        assert (populated_staging / "extracted_data.csv").exists()

    def test_consolidate_correct_column_order(self, populated_staging):
        DataConsolidator(populated_staging).consolidate()
        rows = list(csv.reader((populated_staging / "extracted_data.csv").open()))
        assert rows[0] == [
            "1", "Thu Aug 19 21:54:38 2021", "174434", "four-wheeler",
            "4", "4856", "PC7C042B7", "PTE", "VC965"
        ]

    def test_consolidate_row_count(self, populated_staging):
        DataConsolidator(populated_staging).consolidate()
        rows = list(csv.reader((populated_staging / "extracted_data.csv").open()))
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# DataTransformer
# ---------------------------------------------------------------------------
class TestDataTransformer:

    def test_transform_creates_file(self, populated_staging):
        DataTransformer(populated_staging).transform()
        assert (populated_staging / "transformed_data.csv").exists()

    def test_transform_vehicle_type_uppercase(self, populated_staging):
        DataTransformer(populated_staging).transform()
        rows = list(csv.reader((populated_staging / "transformed_data.csv").open()))
        assert rows[0][3] == "FOUR-WHEELER"
        assert rows[1][3] == "SIX-WHEELER"

    def test_transform_other_fields_unchanged(self, populated_staging):
        DataTransformer(populated_staging).transform()
        rows = list(csv.reader((populated_staging / "transformed_data.csv").open()))
        assert rows[0][0] == "1"
        assert rows[0][8] == "VC965"