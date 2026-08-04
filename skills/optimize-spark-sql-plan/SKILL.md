---
name: optimize-spark-sql-plan
description: Optimize Apache Spark SQL and DataFrame queries using the final Adaptive Query Execution plan and runtime statistics rather than source code alone. Use to reduce runtime, shuffle, spill, scan cost, skew, join amplification, Python UDF overhead, poor partitioning, or unnecessary work while preserving query semantics.
---

# Optimize a Spark SQL plan

Find the highest-impact improvement supported by the executed plan and its runtime metrics. Work from the final adaptive plan (`isFinalPlan=true`), not source code or the initial plan: AQE may already have broadcast, coalesced, or split what you were about to recommend.

## Get the evidence

```bash
export SPARK_HISTORY_URL="https://history.example.com"
python3 scripts/spark_history_api.py sql-list --app-id <application-id>
python3 scripts/spark_history_api.py sql --app-id <application-id> --execution-id <execution-id> > /tmp/spark-sql.json
```

Run from this skill directory. If `SPARK_HISTORY_URL` is unset, find the server before asking the user: try `http://localhost:18080`, a running application's UI on `http://localhost:4040`, and the history-server or eventLog settings in the local Spark config; ask only when nothing responds. Authentication comes from `SPARK_HISTORY_AUTHORIZATION`, `SPARK_HISTORY_COOKIE`, or `SPARK_HISTORY_HEADERS_JSON`; never ask for credentials in chat and never disable TLS verification (`--ca-file` for a private CA).

The snapshot's layout: `sqlExecution.planDescription` is the executed plan text, `sqlExecution.nodes[].metrics` carry per-operator counters with `edges` giving parent-child links, `jobs[].stageIds` tie the execution to stages, `longestStages[]` holds each stage with its `taskSummary` quantiles (arrays align with `quantiles`; middle is median, last is max), and `environment.sparkProperties` is the effective configuration. For data beyond the profile, other subcommands (`slow`, `failure`, `applications`) and the raw `$SPARK_HISTORY_URL/api/v1` endpoints (`/stages/{stage}/{attempt}/taskSummary`, `/allexecutors`, `/environment`) are available with the same auth. If the API omits the plan, recover it from the driver log or generate `explain(mode="formatted")` from the deployed code yourself. Get the deployed query code too: the plan tells you what ran, the code tells you what was intended.

## Highest-impact opportunities, in order, and how to check each

1. **Cardinality blowup.** Join output far exceeding both inputs is usually an unintended many-to-many:

   ```bash
   jq '.sqlExecution.nodes[] | select(.nodeName | test("Join")) | {nodeId, nodeName, rows: [.metrics[] | select(.name | test("output rows"))]}' /tmp/spark-sql.json
   ```

   Compare each join's rows against its children's (follow `edges`). Fix keys or deduplicate first; it dominates every downstream metric.

2. **Pruning.** Scans reading far more than the query uses:

   ```bash
   jq '.sqlExecution.nodes[] | select(.nodeName | startswith("Scan")) | {nodeName, metrics}' /tmp/spark-sql.json
   ```

   Compare files and bytes read against rows output, and check the pushed/partition filters on the scan in `planDescription`.

3. **Broadcast.** A `SortMergeJoin` whose smaller side is observed (not estimated) to be broadcastable:

   ```bash
   jq -r '.sqlExecution.planDescription' /tmp/spark-sql.json | grep -n 'SortMergeJoin\|BroadcastHashJoin'
   ```

   Read the smaller side's "data size" metric from its node, and `spark.sql.autoBroadcastJoinThreshold` from `environment.sparkProperties`. Stale statistics are the usual reason AQE missed it; confirm executor memory headroom before recommending.

4. **Unnecessary shuffles.** Exchanges whose partitioning an upstream operation already satisfies, or repartitions the query does not need:

   ```bash
   jq -r '.sqlExecution.planDescription' /tmp/spark-sql.json | grep -n 'Exchange'
   ```

   Every exchange should map to a semantic requirement; look for back-to-back exchange/sort pairs and matching partitioning expressions above and below.

5. **Skewed joins and aggregations.** One partition dominating a join or aggregate stage:

   ```bash
   jq '{jobStages: [.jobs[] | {jobId, stageIds}], stages: [.longestStages[] | {id: .stage.stageId, run: .taskSummary.executorRunTime, shuffleRead: .taskSummary.shuffleReadMetrics.readBytes}]}' /tmp/spark-sql.json
   ```

   Max many times median on the join's stage is skew; check `planDescription` for `skewed=true` and, if AQE did not handle it, why. Re-run with a higher `--stage-limit` if the stage is missing.

6. **Partition count.** Uniformly oversized shuffle partitions with spill: `grep -n 'AQEShuffleRead' <(jq -r '.sqlExecution.planDescription' /tmp/spark-sql.json)` for coalescing, stage `numTasks` from `longestStages[].stage`, and spill from the check below. Raise `spark.sql.shuffle.partitions` or AQE targets before adding explicit repartitions, which can insert redundant exchanges.

7. **Excess spill in stateful operators.** Aggregates, sorts, and windows spilling:

   ```bash
   jq '.sqlExecution.nodes[] | select(.nodeName | test("Aggregate|Sort|Window")) | {nodeName, spill: [.metrics[] | select(.name | test("spill"))]}' /tmp/spark-sql.json
   ```

   Reduce state (partial aggregation, narrower rows, fewer window columns) before adding memory.

8. **Python UDF boundaries.** Serialization often costs more than the function itself:

   ```bash
   jq '.sqlExecution.nodes[] | select(.nodeName | test("Python")) | {nodeName, metrics}' /tmp/spark-sql.json
   ```

   Weigh node time against rows processed; a native expression removes the boundary.

9. **Caching.** `jq -r '.sqlExecution.planDescription' /tmp/spark-sql.json | grep -c 'InMemoryTableScan'`: a cached result scanned once wastes memory, and a subtree recomputed under several plan branches wants a cache.

The commands above are starting points, not limits: compose your own jq, call the REST API directly, or pull logs, code, and platform state when a question needs it. Rule each candidate in or out with evidence, and when a signal is suggestive but not conclusive, go a level deeper (more task samples, the exact log lines, the executed plan) until you are confident it is or is not the cause. Do not settle for the first plausible explanation.

An `Exchange` is not automatically waste, a sort-merge join is not automatically wrong, a broadcast is not automatically safe, and a Python UDF is not automatically material: tie every recommendation to the node's runtime metrics, and check whether Catalyst or AQE already applied it.

Changes must preserve semantics: row counts, join cardinality, null behavior, duplicates, and ordering assumptions. Changing partitioning can alter low-order bits of floating-point aggregations.

## Report

State the highest-impact change with its plan node, runtime evidence, and expected effect; why Spark did not already do it; correctness risks; and any remaining smaller findings. Do not apply hints, code, or configuration changes to production without approval.
