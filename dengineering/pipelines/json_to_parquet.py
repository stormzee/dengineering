"""JSON-to-Parquet Transformation Pipeline.

Reads one or more JSON (or JSON-lines) files and writes each dataset as a
Parquet file using ``pyarrow``.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class JSONToParquetPipeline:
    """Convert JSON / JSON-lines files to Parquet format.

    Parameters
    ----------
    compression:
        Parquet compression codec.  Supported values: ``'snappy'``,
        ``'gzip'``, ``'brotli'``, ``'zstd'``, ``'none'``.  Default
        ``'snappy'``.
    jsonlines:
        When *True*, each line of the input file is treated as a separate
        JSON object (JSON-lines / NDJSON format).  Default ``False``.
    """

    def __init__(
        self,
        compression: str = "snappy",
        jsonlines: bool = False,
    ) -> None:
        self.compression = compression
        self.jsonlines = jsonlines

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        source: Union[str, Path],
        destination: Union[str, Path],
        schema: Optional[pa.Schema] = None,
    ) -> Path:
        """Transform a JSON file and write it as Parquet.

        Parameters
        ----------
        source:
            Path to the input JSON or JSON-lines file.
        destination:
            Path for the output Parquet file.
        schema:
            Optional explicit PyArrow schema.  When omitted the schema is
            inferred from the data.

        Returns
        -------
        pathlib.Path
            The path of the written Parquet file.
        """
        source = Path(source)
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)

        records = self._load_json(source)
        table = pa.Table.from_pylist(records, schema=schema)
        pq.write_table(table, destination, compression=self.compression)

        logger.info(
            "Wrote %d rows to %s (compression=%s).",
            len(table),
            destination,
            self.compression,
        )
        return destination

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_json(self, path: Path) -> List[Dict[str, Any]]:
        logger.debug("Loading JSON from %s (jsonlines=%s)", path, self.jsonlines)
        with path.open(encoding="utf-8") as fh:
            if self.jsonlines:
                return [json.loads(line) for line in fh if line.strip()]
            data = json.load(fh)
            if isinstance(data, list):
                return data
            return [data]
