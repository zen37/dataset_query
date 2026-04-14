from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class QueryResult:
    columns: list[str]
    rows: list[list[Any]]
    metadata: dict[str, Any] = field(default_factory=dict)


def render_result(result: QueryResult, output: str = "table") -> None:
    if output == "json":
        payload = [dict(zip(result.columns, row, strict=False)) for row in result.rows]
        print(json.dumps(payload, indent=2, default=str))
        return

    render_table(result)


def render_table(result: QueryResult) -> None:
    if not result.columns:
        print("Query finished.")
        return

    if not result.rows:
        print("No rows returned.")
        return

    string_rows = [[str(value) for value in row] for row in result.rows]
    widths = [len(column) for column in result.columns]

    for row in string_rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    header = " | ".join(column.ljust(widths[index]) for index, column in enumerate(result.columns))
    separator = "-+-".join("-" * width for width in widths)

    print(header)
    print(separator)
    for row in string_rows:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)))
