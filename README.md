# dengineering

A collection of data engineering pipelines written in Python.

## Pipelines

| Pipeline | Description |
|---|---|
| `CSVIngestionPipeline` | Read one or more CSV files into a list of dicts. |
| `JSONToParquetPipeline` | Convert JSON / JSON-lines files to Parquet format via PyArrow. |
| `DataValidationPipeline` | Validate records against declarative field rules (type, range, pattern, …). |
| `ETLPipeline` | Composable Extract → Transform → Load pipeline with built-in transformers. |

## Installation

```bash
pip install -e .
```

Requires Python ≥ 3.9 and `pyarrow`.

## Quick Start

### CSV Ingestion

```python
from dengineering.pipelines import CSVIngestionPipeline

pipeline = CSVIngestionPipeline()
records = pipeline.run("data/sales.csv")
# [{'product': 'Widget', 'qty': '10', 'price': '9.99'}, ...]
```

### JSON → Parquet

```python
from dengineering.pipelines import JSONToParquetPipeline

pipeline = JSONToParquetPipeline(compression="snappy")
pipeline.run("data/events.json", "output/events.parquet")
```

### Data Validation

```python
from dengineering.pipelines import DataValidationPipeline
from dengineering.pipelines.validation import FieldRule

rules = [
    FieldRule(name="email", required=True, pattern=r"[^@]+@[^@]+\.[^@]+"),
    FieldRule(name="age", required=True, dtype=int, min_value=0, max_value=120),
]
pipeline = DataValidationPipeline(rules=rules)
report = pipeline.run(records)
if not report.is_valid:
    for err in report.errors:
        print(err)
```

### ETL Pipeline

```python
from dengineering.pipelines import ETLPipeline
from dengineering.pipelines.etl import cast_fields, filter_records, drop_fields

def extract():
    return [{"id": "1", "name": "Alice", "age": "30"}, ...]

def load(batch):
    # write to database, file, etc.
    ...

pipeline = ETLPipeline(
    extractor=extract,
    transformers=[
        cast_fields(age=int),
        filter_records(lambda r: r["age"] >= 18),
        drop_fields("internal_flag"),
    ],
    loader=load,
    batch_size=500,
)
pipeline.run()
```

## Running Tests

```bash
pip install -e ".[dev]"
pytest
```
