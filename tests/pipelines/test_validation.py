"""Tests for DataValidationPipeline."""

import pytest

from dengineering.pipelines.validation import (
    DataValidationPipeline,
    FieldRule,
    ValidationReport,
)


RECORDS = [
    {"name": "Alice", "age": 30, "email": "alice@example.com"},
    {"name": "Bob", "age": 17, "email": "not-an-email"},
    {"name": "", "age": None, "email": "carol@example.com"},
]


def test_valid_records() -> None:
    records = [{"score": 80}, {"score": 95}]
    rules = [FieldRule(name="score", required=True, min_value=0, max_value=100)]
    pipeline = DataValidationPipeline(rules=rules)
    report = pipeline.run(records)
    assert report.is_valid
    assert report.total_rows == 2


def test_required_field_missing() -> None:
    records = [{"name": "Alice"}, {"name": ""}]
    rules = [FieldRule(name="name", required=True)]
    pipeline = DataValidationPipeline(rules=rules)
    report = pipeline.run(records)
    assert not report.is_valid
    assert any(e.rule == "required" for e in report.errors)


def test_dtype_check() -> None:
    records = [{"age": "not-a-number"}]
    rules = [FieldRule(name="age", dtype=int)]
    pipeline = DataValidationPipeline(rules=rules)
    report = pipeline.run(records)
    assert not report.is_valid
    assert report.errors[0].rule == "dtype"


def test_pattern_check() -> None:
    records = [{"email": "bad-email"}, {"email": "good@example.com"}]
    rules = [FieldRule(name="email", pattern=r"[^@]+@[^@]+\.[^@]+")]
    pipeline = DataValidationPipeline(rules=rules)
    report = pipeline.run(records)
    assert report.error_count == 1
    assert report.errors[0].field == "email"


def test_min_max_value() -> None:
    records = [{"age": 150}, {"age": -1}, {"age": 25}]
    rules = [FieldRule(name="age", min_value=0, max_value=120)]
    pipeline = DataValidationPipeline(rules=rules)
    report = pipeline.run(records)
    assert report.error_count == 2


def test_allowed_values() -> None:
    records = [{"status": "active"}, {"status": "unknown"}]
    rules = [FieldRule(name="status", allowed_values=["active", "inactive"])]
    pipeline = DataValidationPipeline(rules=rules)
    report = pipeline.run(records)
    assert report.error_count == 1
    assert report.errors[0].rule == "allowed_values"


def test_custom_rule() -> None:
    records = [{"value": 4}, {"value": 7}]
    rules = [FieldRule(name="value", custom=lambda v: v % 2 == 0)]
    pipeline = DataValidationPipeline(rules=rules)
    report = pipeline.run(records)
    assert report.error_count == 1
    assert report.errors[0].rule == "custom"


def test_fail_fast() -> None:
    records = [{"x": None}, {"x": None}, {"x": None}]
    rules = [FieldRule(name="x", required=True)]
    pipeline = DataValidationPipeline(rules=rules, fail_fast=True)
    report = pipeline.run(records)
    assert report.error_count == 1


def test_empty_records() -> None:
    pipeline = DataValidationPipeline(rules=[FieldRule(name="id", required=True)])
    report = pipeline.run([])
    assert report.is_valid
    assert report.total_rows == 0
