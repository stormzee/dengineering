"""Tests for JSONToParquetPipeline."""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from dengineering.pipelines.json_to_parquet import JSONToParquetPipeline


@pytest.fixture()
def json_file(tmp_path: Path) -> Path:
    path = tmp_path / "data.json"
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


@pytest.fixture()
def jsonlines_file(tmp_path: Path) -> Path:
    path = tmp_path / "data.jsonl"
    lines = [
        json.dumps({"id": 1, "name": "Carol"}),
        json.dumps({"id": 2, "name": "Dan"}),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_run_json_array(json_file: Path, tmp_path: Path) -> None:
    out = tmp_path / "out.parquet"
    pipeline = JSONToParquetPipeline(compression="snappy")
    result = pipeline.run(json_file, out)
    assert result == out
    table = pq.read_table(out)
    assert table.num_rows == 2


def test_run_jsonlines(jsonlines_file: Path, tmp_path: Path) -> None:
    out = tmp_path / "out.parquet"
    pipeline = JSONToParquetPipeline(jsonlines=True)
    pipeline.run(jsonlines_file, out)
    table = pq.read_table(out)
    assert table.num_rows == 2
    assert "name" in table.schema.names


def test_creates_destination_dir(json_file: Path, tmp_path: Path) -> None:
    out = tmp_path / "subdir" / "nested" / "out.parquet"
    pipeline = JSONToParquetPipeline()
    pipeline.run(json_file, out)
    assert out.exists()


def test_single_object_json(tmp_path: Path) -> None:
    src = tmp_path / "single.json"
    src.write_text(json.dumps({"id": 99, "value": 3.14}), encoding="utf-8")
    out = tmp_path / "single.parquet"
    pipeline = JSONToParquetPipeline()
    pipeline.run(src, out)
    table = pq.read_table(out)
    assert table.num_rows == 1
