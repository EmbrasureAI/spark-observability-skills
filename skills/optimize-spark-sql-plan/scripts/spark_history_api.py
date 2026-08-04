#!/usr/bin/env python3
"""Read bounded diagnostic evidence from the Spark History Server REST API."""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import sys
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

MAX_RESPONSE_BYTES = 25 * 1024 * 1024
SENSITIVE = re.compile(r"(secret|password|token|credential|access[._-]?key|private[._-]?key|keytab|cookie)", re.I)


class HistoryClient:
    def __init__(self, base_url: str, timeout: float, ca_file: str | None) -> None:
        base = base_url.rstrip("/")
        self.base = base if base.endswith("/api/v1") else f"{base}/api/v1"
        self.timeout = timeout
        self.context = ssl.create_default_context(cafile=ca_file)
        self.headers = {"Accept": "application/json", "User-Agent": "spark-history-skill/1"}
        authorization = os.getenv("SPARK_HISTORY_AUTHORIZATION")
        cookie = os.getenv("SPARK_HISTORY_COOKIE")
        if authorization:
            self.headers["Authorization"] = authorization
        if cookie:
            self.headers["Cookie"] = cookie
        extra = os.getenv("SPARK_HISTORY_HEADERS_JSON")
        if extra:
            parsed = json.loads(extra)
            if not isinstance(parsed, dict):
                raise ValueError("SPARK_HISTORY_HEADERS_JSON must be a JSON object")
            for key, value in parsed.items():
                if not isinstance(key, str) or not isinstance(value, str) or "\n" in key + value or "\r" in key + value:
                    raise ValueError("History headers must be single-line strings")
                self.headers[key] = value

    def get(self, path: str, **query: Any) -> Any:
        params = [(key, item) for key, value in query.items() if value is not None for item in (value if isinstance(value, list) else [value])]
        url = f"{self.base}/{path.lstrip('/')}"
        if params:
            url += "?" + urlencode(params)
        request = Request(url, headers=self.headers, method="GET")
        try:
            with urlopen(request, timeout=self.timeout, context=self.context) as response:
                length = response.headers.get("Content-Length")
                if length and int(length) > MAX_RESPONSE_BYTES:
                    raise RuntimeError(f"History response exceeds {MAX_RESPONSE_BYTES} bytes")
                body = response.read(MAX_RESPONSE_BYTES + 1)
        except HTTPError as exc:
            raise RuntimeError(f"Spark History API returned HTTP {exc.code} for {path}") from exc
        except URLError as exc:
            raise RuntimeError(f"Could not reach Spark History API for {path}: {exc.reason}") from exc
        if len(body) > MAX_RESPONSE_BYTES:
            raise RuntimeError(f"History response exceeds {MAX_RESPONSE_BYTES} bytes")
        return json.loads(body)


def app_path(app_id: str, suffix: str) -> str:
    return f"applications/{quote(app_id, safe='/')}/{suffix.lstrip('/')}"


def sanitize_environment(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"available": False}
    result: dict[str, Any] = {}
    if "runtime" in payload:
        result["runtime"] = payload["runtime"]
    properties = []
    for entry in payload.get("sparkProperties", []):
        if not isinstance(entry, list) or len(entry) < 2:
            continue
        key, value = str(entry[0]), entry[1]
        properties.append([key, "<redacted>" if SENSITIVE.search(key) else value])
    result["sparkProperties"] = properties
    system = {}
    for entry in payload.get("systemProperties", []):
        if isinstance(entry, list) and len(entry) >= 2 and entry[0] in {"java.version", "java.vendor", "os.name", "os.version", "os.arch"}:
            system[str(entry[0])] = entry[1]
    result["systemProperties"] = system
    return result


def stage_duration_ms(stage: dict[str, Any]) -> float:
    def parse(value: Any) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            return datetime.fromisoformat(value.replace("GMT", "+00:00").replace("Z", "+00:00"))
        except ValueError:
            return None
    start, end = parse(stage.get("submissionTime")), parse(stage.get("completionTime"))
    if start and end:
        return max(0.0, (end - start).total_seconds() * 1000)
    return float(stage.get("executorRunTime") or 0)


def top_stages(stages: Any, limit: int) -> list[dict[str, Any]]:
    if not isinstance(stages, list):
        return []
    return sorted(stages, key=stage_duration_ms, reverse=True)[:limit]


def earliest_stages(stages: Any, limit: int) -> list[dict[str, Any]]:
    if not isinstance(stages, list):
        return []
    return sorted(stages, key=lambda stage: str(stage.get("submissionTime") or ""))[:limit]


def stage_evidence(client: HistoryClient, app_id: str, stages: list[dict[str, Any]], task_limit: int, failed_only: bool) -> list[dict[str, Any]]:
    evidence = []
    for stage in stages:
        stage_id = stage.get("stageId")
        attempt_id = stage.get("attemptId", 0)
        if stage_id is None:
            continue
        root = app_path(app_id, f"stages/{stage_id}/{attempt_id}")
        item = {"stage": stage}
        try:
            item["taskSummary"] = client.get(f"{root}/taskSummary", quantiles="0.0,0.5,0.95,0.99,1.0")
            item["tasks"] = client.get(
                f"{root}/taskList",
                offset=0,
                length=task_limit,
                sortBy=None if failed_only else "-runtime",
                status="failed" if failed_only else None,
            )
        except RuntimeError as exc:
            item["collectionError"] = str(exc)
        evidence.append(item)
    return evidence


def collect_failure(client: HistoryClient, app_id: str, stage_limit: int, task_limit: int) -> dict[str, Any]:
    failed_stages = client.get(app_path(app_id, "stages"), status="failed")
    return {
        "profile": "failure",
        "applicationId": app_id,
        "failedJobs": client.get(app_path(app_id, "jobs"), status="failed"),
        "failedStages": stage_evidence(client, app_id, earliest_stages(failed_stages, stage_limit), task_limit, True),
        "allExecutors": client.get(app_path(app_id, "allexecutors")),
        "environment": sanitize_environment(client.get(app_path(app_id, "environment"))),
        "sqlExecutions": client.get(app_path(app_id, "sql"), details="false", planDescription="false", offset=0, length=100),
        "limitations": ["Driver logs and cluster-manager termination details are not part of this REST snapshot."],
    }


def collect_slow(client: HistoryClient, app_id: str, stage_limit: int, task_limit: int) -> dict[str, Any]:
    stages = client.get(
        app_path(app_id, "stages"),
        withSummaries="true",
        quantiles="0.0,0.5,0.95,0.99,1.0",
    )
    selected = top_stages(stages, stage_limit)
    return {
        "profile": "slow",
        "applicationId": app_id,
        "jobs": client.get(app_path(app_id, "jobs")),
        "longestStages": stage_evidence(client, app_id, selected, task_limit, False),
        "allExecutors": client.get(app_path(app_id, "allexecutors")),
        "environment": sanitize_environment(client.get(app_path(app_id, "environment"))),
        "sqlExecutions": client.get(app_path(app_id, "sql"), details="false", planDescription="false", offset=0, length=100),
    }


def collect_sql(client: HistoryClient, app_id: str, execution_id: str, stage_limit: int, task_limit: int) -> dict[str, Any]:
    stages = client.get(
        app_path(app_id, "stages"),
        withSummaries="true",
        quantiles="0.0,0.5,0.95,0.99,1.0",
    )
    return {
        "profile": "sql",
        "applicationId": app_id,
        "executionId": execution_id,
        "sqlExecution": client.get(
            app_path(app_id, f"sql/{quote(execution_id, safe='')}"),
            details="true",
            planDescription="true",
        ),
        "jobs": client.get(app_path(app_id, "jobs")),
        "longestStages": stage_evidence(client, app_id, top_stages(stages, stage_limit), task_limit, False),
        "environment": sanitize_environment(client.get(app_path(app_id, "environment"))),
    }


def parser() -> argparse.ArgumentParser:
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--base-url", default=os.getenv("SPARK_HISTORY_URL"), help="History Server root or /api/v1 URL; defaults to SPARK_HISTORY_URL")
    shared.add_argument("--timeout", type=float, default=20.0)
    shared.add_argument("--ca-file", default=None, help="Custom CA bundle; TLS verification remains enabled")
    shared.add_argument("--stage-limit", type=int, default=8)
    shared.add_argument("--task-limit", type=int, default=20)
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    apps = sub.add_parser("applications", parents=[shared])
    apps.add_argument("--status", choices=["completed", "running"])
    apps.add_argument("--limit", type=int, default=50)
    sql_list = sub.add_parser("sql-list", parents=[shared])
    sql_list.add_argument("--app-id", required=True)
    sql_list.add_argument("--limit", type=int, default=100)
    for name in ("failure", "slow", "sql"):
        command = sub.add_parser(name, parents=[shared])
        command.add_argument("--app-id", required=True)
        if name == "sql":
            command.add_argument("--execution-id", required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    if not args.base_url:
        raise SystemExit("Set --base-url or SPARK_HISTORY_URL")
    if not 1 <= args.stage_limit <= 50 or not 1 <= args.task_limit <= 200:
        raise SystemExit("Limits out of range: stage 1-50, task 1-200")
    client = HistoryClient(args.base_url, args.timeout, args.ca_file)
    if args.command == "applications":
        payload = client.get("applications", status=args.status, limit=args.limit)
    elif args.command == "sql-list":
        payload = client.get(
            app_path(args.app_id, "sql"),
            details="false",
            planDescription="false",
            offset=0,
            length=args.limit,
        )
    elif args.command == "failure":
        payload = collect_failure(client, args.app_id, args.stage_limit, args.task_limit)
    elif args.command == "slow":
        payload = collect_slow(client, args.app_id, args.stage_limit, args.task_limit)
    else:
        payload = collect_sql(client, args.app_id, args.execution_id, args.stage_limit, args.task_limit)
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
