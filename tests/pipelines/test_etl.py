"""Tests for ETLPipeline and built-in transformers."""

from typing import Any, Dict, List, Optional

import pytest

from dengineering.pipelines.etl import (
    ETLPipeline,
    cast_fields,
    drop_fields,
    filter_records,
    rename_fields,
)

Record = Dict[str, Any]

SAMPLE_RECORDS: List[Record] = [
    {"id": "1", "name": "Alice", "age": "30", "active": "true"},
    {"id": "2", "name": "Bob", "age": "17", "active": "false"},
    {"id": "3", "name": "Carol", "age": "25", "active": "true"},
]


def make_extractor(records: List[Record]):
    def _extract():
        return records
    return _extract


# ---------------------------------------------------------------------------
# Transformer unit tests
# ---------------------------------------------------------------------------


def test_rename_fields() -> None:
    t = rename_fields(name="full_name")
    result = t({"name": "Alice", "age": 30})
    assert "full_name" in result
    assert "name" not in result
    assert result["age"] == 30


def test_cast_fields() -> None:
    t = cast_fields(age=int, salary=float)
    result = t({"age": "30", "salary": "12345.67", "name": "Alice"})
    assert result["age"] == 30
    assert result["salary"] == pytest.approx(12345.67)
    assert result["name"] == "Alice"


def test_cast_fields_bad_value(caplog) -> None:
    t = cast_fields(age=int)
    result = t({"age": "not-a-number"})
    assert result["age"] == "not-a-number"  # unchanged on failure


def test_drop_fields() -> None:
    t = drop_fields("secret", "internal")
    result = t({"name": "Alice", "secret": "hidden", "internal": "x", "age": 30})
    assert "secret" not in result
    assert "internal" not in result
    assert result["name"] == "Alice"


def test_filter_records_keep() -> None:
    t = filter_records(lambda r: r["age"] >= 18)
    assert t({"age": 25}) == {"age": 25}


def test_filter_records_discard() -> None:
    t = filter_records(lambda r: r["age"] >= 18)
    assert t({"age": 10}) is None


# ---------------------------------------------------------------------------
# ETLPipeline tests
# ---------------------------------------------------------------------------


def test_pipeline_no_transformers() -> None:
    pipeline = ETLPipeline(extractor=make_extractor(SAMPLE_RECORDS))
    result = pipeline.run()
    assert len(result) == 3


def test_pipeline_with_transformers() -> None:
    transformers = [
        cast_fields(age=int),
        filter_records(lambda r: r["age"] >= 18),
        drop_fields("active"),
    ]
    pipeline = ETLPipeline(
        extractor=make_extractor(SAMPLE_RECORDS),
        transformers=transformers,
    )
    result = pipeline.run()
    assert len(result) == 2
    assert all("active" not in r for r in result)
    assert all(isinstance(r["age"], int) for r in result)


def test_pipeline_loader_called() -> None:
    loaded: List[List[Record]] = []

    def my_loader(records: List[Record]) -> None:
        loaded.append(list(records))

    pipeline = ETLPipeline(
        extractor=make_extractor(SAMPLE_RECORDS),
        loader=my_loader,
    )
    pipeline.run()
    assert len(loaded) == 1
    assert len(loaded[0]) == 3


def test_pipeline_batch_loader() -> None:
    batches: List[List[Record]] = []

    def my_loader(records: List[Record]) -> None:
        batches.append(list(records))

    pipeline = ETLPipeline(
        extractor=make_extractor(SAMPLE_RECORDS),
        loader=my_loader,
        batch_size=2,
    )
    pipeline.run()
    assert len(batches) == 2  # 3 records, batch_size=2 → 2 batches
    assert len(batches[0]) == 2
    assert len(batches[1]) == 1


def test_pipeline_empty_source() -> None:
    pipeline = ETLPipeline(extractor=make_extractor([]))
    result = pipeline.run()
    assert result == []
