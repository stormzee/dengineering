"""Data Validation Pipeline.

Validates a list of records against a set of declarative rules and returns
a :class:`ValidationReport` describing any failures.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rule types
# ---------------------------------------------------------------------------


@dataclass
class FieldRule:
    """Validation rule for a single field.

    Parameters
    ----------
    name:
        Column / key name to validate.
    required:
        When *True* the field must be present and non-empty.
    dtype:
        Expected Python type (e.g. ``int``, ``float``, ``str``).
    pattern:
        Regular-expression pattern the string value must fully match.
    min_value:
        Minimum numeric value (inclusive).
    max_value:
        Maximum numeric value (inclusive).
    allowed_values:
        Whitelist of accepted values.
    custom:
        Arbitrary callable ``(value) -> bool``; rule fails when it returns
        *False*.
    """

    name: str
    required: bool = False
    dtype: Optional[type] = None
    pattern: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[Any]] = None
    custom: Optional[Callable[[Any], bool]] = None


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class ValidationError:
    """A single validation failure."""

    row_index: int
    field: str
    rule: str
    message: str


@dataclass
class ValidationReport:
    """Aggregated result of a validation run."""

    total_rows: int
    errors: List[ValidationError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    @property
    def error_count(self) -> int:
        return len(self.errors)

    def __str__(self) -> str:  # pragma: no cover
        if self.is_valid:
            return f"ValidationReport: OK ({self.total_rows} rows)"
        return (
            f"ValidationReport: {self.error_count} error(s) in "
            f"{self.total_rows} rows"
        )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class DataValidationPipeline:
    """Validate records against a set of :class:`FieldRule` objects.

    Parameters
    ----------
    rules:
        List of :class:`FieldRule` objects to apply to every record.
    fail_fast:
        When *True*, stop processing after the first error (default *False*).
    """

    def __init__(
        self,
        rules: List[FieldRule],
        fail_fast: bool = False,
    ) -> None:
        self.rules = rules
        self.fail_fast = fail_fast

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, records: List[Dict[str, Any]]) -> ValidationReport:
        """Validate *records* and return a :class:`ValidationReport`.

        Parameters
        ----------
        records:
            Iterable of dicts to validate.

        Returns
        -------
        ValidationReport
        """
        report = ValidationReport(total_rows=len(records))

        for idx, record in enumerate(records):
            for rule in self.rules:
                errors = self._check_rule(idx, record, rule)
                report.errors.extend(errors)
                if self.fail_fast and report.errors:
                    return report

        logger.info(
            "Validated %d rows; %d error(s) found.",
            report.total_rows,
            report.error_count,
        )
        return report

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _check_rule(
        self,
        idx: int,
        record: Dict[str, Any],
        rule: FieldRule,
    ) -> List[ValidationError]:
        errors: List[ValidationError] = []
        value = record.get(rule.name)

        # Required
        if rule.required and (value is None or value == ""):
            errors.append(
                ValidationError(
                    row_index=idx,
                    field=rule.name,
                    rule="required",
                    message=f"Field '{rule.name}' is required but missing or empty.",
                )
            )
            return errors  # skip further checks if value is absent

        if value is None or value == "":
            return errors

        # dtype
        if rule.dtype is not None and not isinstance(value, rule.dtype):
            errors.append(
                ValidationError(
                    row_index=idx,
                    field=rule.name,
                    rule="dtype",
                    message=(
                        f"Field '{rule.name}' expected type {rule.dtype.__name__}, "
                        f"got {type(value).__name__}."
                    ),
                )
            )

        # pattern
        if rule.pattern is not None:
            if not re.fullmatch(rule.pattern, str(value)):
                errors.append(
                    ValidationError(
                        row_index=idx,
                        field=rule.name,
                        rule="pattern",
                        message=(
                            f"Field '{rule.name}' value '{value}' does not match "
                            f"pattern '{rule.pattern}'."
                        ),
                    )
                )

        # numeric bounds
        if rule.min_value is not None or rule.max_value is not None:
            try:
                numeric = float(value)  # type: ignore[arg-type]
                if rule.min_value is not None and numeric < rule.min_value:
                    errors.append(
                        ValidationError(
                            row_index=idx,
                            field=rule.name,
                            rule="min_value",
                            message=(
                                f"Field '{rule.name}' value {numeric} is below "
                                f"minimum {rule.min_value}."
                            ),
                        )
                    )
                if rule.max_value is not None and numeric > rule.max_value:
                    errors.append(
                        ValidationError(
                            row_index=idx,
                            field=rule.name,
                            rule="max_value",
                            message=(
                                f"Field '{rule.name}' value {numeric} exceeds "
                                f"maximum {rule.max_value}."
                            ),
                        )
                    )
            except (TypeError, ValueError):
                errors.append(
                    ValidationError(
                        row_index=idx,
                        field=rule.name,
                        rule="numeric",
                        message=(
                            f"Field '{rule.name}' value '{value}' is not numeric."
                        ),
                    )
                )

        # allowed_values
        if rule.allowed_values is not None and value not in rule.allowed_values:
            errors.append(
                ValidationError(
                    row_index=idx,
                    field=rule.name,
                    rule="allowed_values",
                    message=(
                        f"Field '{rule.name}' value '{value}' is not in the "
                        f"allowed list {rule.allowed_values}."
                    ),
                )
            )

        # custom
        if rule.custom is not None:
            try:
                ok = rule.custom(value)
            except Exception as exc:
                ok = False
                logger.warning("Custom rule for '%s' raised: %s", rule.name, exc)
            if not ok:
                errors.append(
                    ValidationError(
                        row_index=idx,
                        field=rule.name,
                        rule="custom",
                        message=f"Field '{rule.name}' failed the custom validation rule.",
                    )
                )

        return errors
