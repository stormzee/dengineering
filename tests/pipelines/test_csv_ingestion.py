"""Tests for CSVIngestionPipeline."""

import csv
import tempfile
from pathlib import Path

import pytest

from dengineering.pipelines.csv_ingestion import CSVIngestionPipeline


@pytest.fixture()
def csv_file(tmp_path: Path) -> Path:
    path = tmp_path / "data.csv"
    path.write_text("name,age,city\nAlice,30,NYC\nBob,25,LA\n", encoding="utf-8")
    return path


@pytest.fixture()
def csv_file_semicolon(tmp_path: Path) -> Path:
    path = tmp_path / "data_semi.csv"
    path.write_text("name;age\nCarol;40\nDan;35\n", encoding="utf-8")
    return path


def test_run_single_file(csv_file: Path) -> None:
    pipeline = CSVIngestionPipeline()
    records = pipeline.run(csv_file)
    assert len(records) == 2
    assert records[0] == {"name": "Alice", "age": "30", "city": "NYC"}
    assert records[1] == {"name": "Bob", "age": "25", "city": "LA"}


def test_run_multiple_files(csv_file: Path, tmp_path: Path) -> None:
    second = tmp_path / "extra.csv"
    second.write_text("name,age,city\nEve,28,SF\n", encoding="utf-8")
    pipeline = CSVIngestionPipeline()
    records = pipeline.run([csv_file, second])
    assert len(records) == 3


def test_run_path_string(csv_file: Path) -> None:
    pipeline = CSVIngestionPipeline()
    records = pipeline.run(str(csv_file))
    assert len(records) == 2


def test_custom_delimiter(csv_file_semicolon: Path) -> None:
    pipeline = CSVIngestionPipeline(delimiter=";")
    records = pipeline.run(csv_file_semicolon)
    assert len(records) == 2
    assert records[0]["name"] == "Carol"


def test_skip_blank_lines(tmp_path: Path) -> None:
    path = tmp_path / "blank.csv"
    path.write_text("name,age\nAlice,30\n,,\nBob,25\n", encoding="utf-8")
    pipeline = CSVIngestionPipeline(skip_blank_lines=True)
    records = pipeline.run(path)
    assert len(records) == 2


def test_stream(csv_file: Path) -> None:
    pipeline = CSVIngestionPipeline()
    records = list(pipeline.stream(csv_file))
    assert len(records) == 2
