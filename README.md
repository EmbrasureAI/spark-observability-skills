# Spark observability skills

Open-source agent skills from [Embrasure](https://embrasure.ai) for diagnosing and optimizing [Apache Spark](https://spark.apache.org) workloads. Each skill is a single `SKILL.md` with an ordered list of the highest-impact causes to check, plus a read-only Spark History Server REST client under `scripts/` that collects the runtime evidence in one bounded snapshot.

## Install

```bash
git clone https://github.com/EmbrasureAI/spark-observability-skills.git
ln -s "$PWD"/spark-observability-skills/skills/* ~/.codex/skills/
```

Point the symlinks at whichever skills directory your harness reads (`~/.codex/skills`, `~/.claude/skills`, ...), creating it first if needed, then restart or reload the harness.

Or paste this into your agent:

> Clone https://github.com/EmbrasureAI/spark-observability-skills and symlink each directory under `skills/` into your skills directory, then tell me to reload.

## Setup

The skills need HTTP access to a Spark History Server, or to the live UI of a running application (the driver UI on port 4040 serves the same REST API):

```bash
export SPARK_HISTORY_URL="https://<your-history-server>"   # e.g. http://localhost:18080 locally or via a tunnel
export SPARK_HISTORY_AUTHORIZATION="Bearer <token>"        # only if the server requires auth
```

- History data exists only for applications that ran with `spark.eventLog.enabled=true`.
- If the server is cluster-internal, open a tunnel first, for example `kubectl port-forward svc/spark-history-server 18080:18080`.
- Behind an SSO proxy, reuse your browser session with `SPARK_HISTORY_COOKIE` or `SPARK_HISTORY_HEADERS_JSON`; pass `--ca-file` for a private CA.

## Skills

- [Debug Spark failures](skills/debug-spark-failure/SKILL.md): a run failed. Trace driver and executor crashes, out-of-memory kills, fetch failures, task exceptions, and aborted stages back to the earliest supported cause instead of the last retry error.
- [Debug slow Spark jobs](skills/debug-slow-spark-job/SKILL.md): a run is slower or more expensive than it should be. Compare against a healthy run to localize the first divergence: skew, shuffle, spill, GC, poor parallelism, scheduler delay, or infrastructure.
- [Optimize Spark SQL plans](skills/optimize-spark-sql-plan/SKILL.md): a query works but costs too much. Read its final adaptive plan and runtime metrics to cut scans, shuffles, joins, and unnecessary work without changing query results.

## Safety

The collector is read-only, bounds large responses by default, redacts sensitive Spark properties, and keeps TLS verification enabled. Review every command against your environment and access policies before running it.

## Contributing

Each skill directory is self-contained so it can be symlinked or copied on its own. As a result, `scripts/spark_history_api.py` is intentionally identical across the three skills. If you change one copy, sync all three.

## License

Apache-2.0. Apache Spark, Apache Celeborn, and their respective marks belong to the Apache Software Foundation. This project is not an official Apache Software Foundation project.
