"""CSV Ingestion Pipeline.

Reads one or more CSV files and returns their contents as a list of
dictionaries (one dict per row).
"""

import csv
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

logger = logging.getLogger(__name__)


class CSVIngestionPipeline:
    """Ingest CSV files into a list of records.

    Parameters
    ----------
    delimiter:
        Field delimiter used in the CSV files (default ``','``).
    encoding:
        File encoding (default ``'utf-8'``).
    skip_blank_lines:
        When *True*, empty rows are discarded (default ``True``).
    """

    def __init__(
        self,
        delimiter: str = ",",
        encoding: str = "utf-8",
        skip_blank_lines: bool = True,
    ) -> None:
        self.delimiter = delimiter
        self.encoding = encoding
        self.skip_blank_lines = skip_blank_lines

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, paths: Union[str, Path, List[Union[str, Path]]]) -> List[Dict[str, Any]]:
        """Read one or more CSV files and return all rows as a flat list.

        Parameters
        ----------
        paths:
            A single file path or a list of file paths to ingest.

        Returns
        -------
        list[dict]
            All rows from every file, in order.
        """
        if isinstance(paths, (str, Path)):
            paths = [paths]

        records: List[Dict[str, Any]] = []
        for path in paths:
            records.extend(self._read_file(Path(path)))
        logger.info("Ingested %d records from %d file(s).", len(records), len(paths))
        return records

    def stream(
        self, paths: Union[str, Path, List[Union[str, Path]]]
    ) -> Iterator[Dict[str, Any]]:
        """Yield rows one at a time (memory-efficient alternative to :meth:`run`).

        Parameters
        ----------
        paths:
            A single file path or a list of file paths to ingest.
        """
        if isinstance(paths, (str, Path)):
            paths = [paths]

        for path in paths:
            yield from self._read_file(Path(path))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _read_file(self, path: Path) -> List[Dict[str, Any]]:
        logger.debug("Reading CSV file: %s", path)
        rows: List[Dict[str, Any]] = []
        with path.open(newline="", encoding=self.encoding) as fh:
            reader = csv.DictReader(fh, delimiter=self.delimiter)
            for row in reader:
                non_none_values = [v for k, v in row.items() if k is not None]
                if self.skip_blank_lines and all(v == "" for v in non_none_values):
                    continue
                rows.append({k: v for k, v in row.items() if k is not None})
        return rows
