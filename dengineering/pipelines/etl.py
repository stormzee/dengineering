"""ETL Pipeline (Extract → Transform → Load).

Provides a composable ETL pipeline that chains an extractor, zero or more
transformers, and a loader.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

Record = Dict[str, Any]
Transformer = Callable[[Record], Optional[Record]]
Extractor = Callable[[], Iterable[Record]]
Loader = Callable[[List[Record]], None]


# ---------------------------------------------------------------------------
# Built-in transformers
# ---------------------------------------------------------------------------


def rename_fields(**mapping: str) -> Transformer:
    """Return a transformer that renames fields according to *mapping*.

    Example
    -------
    >>> t = rename_fields(old_name="new_name")
    """

    def _transform(record: Record) -> Record:
        result = {}
        for key, value in record.items():
            result[mapping.get(key, key)] = value
        return result

    return _transform


def cast_fields(**mapping: type) -> Transformer:
    """Return a transformer that casts fields to the given Python types.

    Example
    -------
    >>> t = cast_fields(age=int, salary=float)
    """

    def _transform(record: Record) -> Optional[Record]:
        result = dict(record)
        for key, target_type in mapping.items():
            if key in result and result[key] is not None and result[key] != "":
                try:
                    result[key] = target_type(result[key])
                except (ValueError, TypeError) as exc:
                    logger.warning(
                        "Could not cast field '%s' to %s: %s",
                        key,
                        target_type.__name__,
                        exc,
                    )
        return result

    return _transform


def drop_fields(*fields: str) -> Transformer:
    """Return a transformer that removes *fields* from every record."""

    def _transform(record: Record) -> Record:
        return {k: v for k, v in record.items() if k not in fields}

    return _transform


def filter_records(predicate: Callable[[Record], bool]) -> Transformer:
    """Return a transformer that drops records not matching *predicate*.

    When *predicate* returns *False* for a record, ``None`` is returned,
    signalling the :class:`ETLPipeline` to skip that record.
    """

    def _transform(record: Record) -> Optional[Record]:
        return record if predicate(record) else None

    return _transform


# ---------------------------------------------------------------------------
# ETL Pipeline
# ---------------------------------------------------------------------------


class ETLPipeline:
    """Extract, Transform, and Load records.

    Parameters
    ----------
    extractor:
        Callable with no arguments that returns an iterable of records.
    transformers:
        Ordered list of single-record transformers.  A transformer is any
        callable ``(record) -> record | None``.  Returning *None* discards
        the record.
    loader:
        Callable that accepts the final list of records and persists them.
    batch_size:
        When set, records are passed to the loader in batches of at most
        *batch_size* rows.  Default *None* (load all at once).
    """

    def __init__(
        self,
        extractor: Extractor,
        transformers: Optional[List[Transformer]] = None,
        loader: Optional[Loader] = None,
        batch_size: Optional[int] = None,
    ) -> None:
        self.extractor = extractor
        self.transformers: List[Transformer] = transformers or []
        self.loader = loader
        self.batch_size = batch_size

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> List[Record]:
        """Execute the full ETL pipeline.

        Returns
        -------
        list[dict]
            The final list of records after all transformations.
        """
        start = time.monotonic()
        logger.info("ETL pipeline starting.")

        records = list(self._transform(self._extract()))

        if self.loader is not None:
            self._load(records)

        elapsed = time.monotonic() - start
        logger.info(
            "ETL pipeline finished: %d record(s) in %.3fs.", len(records), elapsed
        )
        return records

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extract(self) -> Iterator[Record]:
        logger.debug("Extracting records.")
        yield from self.extractor()

    def _transform(self, records: Iterator[Record]) -> Iterator[Record]:
        for record in records:
            result: Optional[Record] = record
            for transformer in self.transformers:
                if result is None:
                    break
                result = transformer(result)
            if result is not None:
                yield result

    def _load(self, records: List[Record]) -> None:
        assert self.loader is not None
        if self.batch_size is None:
            logger.debug("Loading %d records.", len(records))
            self.loader(records)
        else:
            for start in range(0, len(records), self.batch_size):
                batch = records[start : start + self.batch_size]
                logger.debug("Loading batch of %d records.", len(batch))
                self.loader(batch)
