---
name: debug-spark-failure
description: Diagnose failed Apache Spark and PySpark applications from History Server evidence, logs, and cluster-manager state. Use for driver or executor crashes, out-of-memory errors, fetch failures, task exceptions, timeouts, repeated retries, aborted stages, and intermittent production failures.
---

# Debug a Spark failure

Find the root cause: the earliest failure that explains the rest of the chain. Later fetch failures, retries, and executor loss are usually fallout, not cause.

## Get the evidence

```bash
export SPARK_HISTORY_URL="https://history.example.com"
python3 scripts/spark_history_api.py applications --status completed --limit 20
python3 scripts/spark_history_api.py failure --app-id <application-id> > /tmp/spark-failure.json
```

Run from this skill directory. If `SPARK_HISTORY_URL` is unset, find the server before asking the user: try `http://localhost:18080`, a running application's UI on `http://localhost:4040`, and the history-server or eventLog settings in the local Spark config; ask only when nothing responds. Authentication comes from `SPARK_HISTORY_AUTHORIZATION`, `SPARK_HISTORY_COOKIE`, or `SPARK_HISTORY_HEADERS_JSON`; never ask for credentials in chat and never disable TLS verification (`--ca-file` for a private CA).

Other subcommands: `slow --app-id <id>` for longest-stage distributions, `sql-list` / `sql --app-id <id> --execution-id <n>` for the executed plan; all accept `--stage-limit N --task-limit N`. For anything the profiles omit, call `$SPARK_HISTORY_URL/api/v1` directly with the same auth headers: `/applications/{app}/jobs`, `/stages/{stage}/{attempt}/taskSummary?quantiles=0.05,0.5,0.95`, `/stages/{stage}/{attempt}/taskList?status=failed`, `/allexecutors`, `/environment`.

The History Server has no driver logs or container termination reasons: read `spark.master` and `spark.submit.deployMode` from `environment.sparkProperties` to locate them, then pull the driver log around the first exception, the first failing executor's log (`kubectl logs --previous`, `yarn logs -applicationId`, or the platform's log store), and the cluster manager's reason for any lost container. A driver crash can leave no failed Spark job at all. In `taskSummary`, metric arrays align with `quantiles`: the middle entry is the median, the last is the max.

## Likeliest causes, in order, and how to check each

1. **Executor OOM.** Dead executors and why they were removed:

   ```bash
   jq '.allExecutors[] | select(.isActive | not) | {id, hostPort, removeReason}' /tmp/spark-failure.json
   ```

   The decisive record is the cluster manager's exit code: `kubectl describe pod <executor-pod>` (137 / `OOMKilled`) or the YARN container exit status. Then run the skew check below: one oversized partition or join state is the usual cause, not globally low memory.

2. **Skew surfacing as failure.** The task that dies is the one with far more data than the median:

   ```bash
   jq '.failedStages[] | {stage: .stage.stageId, quantiles: .taskSummary.quantiles, run: .taskSummary.executorRunTime, input: .taskSummary.inputMetrics.bytesRead, shuffleRead: .taskSummary.shuffleReadMetrics.readBytes}' /tmp/spark-failure.json
   ```

   If max is many times median, fix the key distribution, not the resources.

3. **Fetch failures.** The error names the executor and shuffle block that could not be fetched:

   ```bash
   jq '.failedStages[].tasks[] | select(.errorMessage != null) | {index, host, executorId, err: .errorMessage[0:200]}' /tmp/spark-failure.json
   ```

   Look the named executor up in `allExecutors` for its `removeReason` and diagnose its earlier death from its own log; the fetch errors are fallout. If `environment` shows Celeborn or an external shuffle service, confirm in the driver log it actually served that shuffle; clients can fall back to built-in shuffle.

4. **Flaky infrastructure.** Failures clustered on one host point at infrastructure; on one partition, at data:

   ```bash
   jq '[.failedStages[].tasks[] | select(.errorMessage != null)] | group_by(.host) | map({host: .[0].host, failures: length})' /tmp/spark-failure.json
   ```

   For a suspect host, check node events (`kubectl get events`, cluster-manager node state), spot preemption, and disk or network errors in that executor's log. Tasks that later succeeded on retry confirm flakiness rather than bad data.

5. **Deterministic data or code failure.** Same exception at the same partition on every attempt:

   ```bash
   jq '[.failedStages[].tasks[] | select(.errorMessage != null)] | group_by(.index) | map({partition: .[0].index, attempts: length, err: .[0].errorMessage[0:120]})' /tmp/spark-failure.json
   ```

   Corrupt input, a pathological record, or a user-code bug; the partition identity is your reproduction.

6. **Python worker death.** Filter the same task samples for `Python worker` in `err`; the traceback is in that executor's stderr, and `spark.executor.pyspark.memory` in `environment.sparkProperties` bounds worker memory.

7. **Driver failure.** `jq '.failedJobs | length' /tmp/spark-failure.json` returning `0` for a failed app means the driver died: read the end of the driver log. Large collect, broadcast, or result sizes and very high partition counts (task-metadata bloat) are common; a GC-stalled driver also produces heartbeat storms in executor logs.

8. **Timeouts.** Heartbeat and RPC timeouts in `err` are almost always secondary: cross-check executor GC (`jq '.allExecutors[] | {id, gc: .totalGCTime, dur: .totalDuration}'`) and the skew check to find what was actually slow instead of raising the timeout.

9. **Commit or write failure.** Committer and destination errors are in the driver log (`grep -iE 'commit|abort' <driver-log>`) and the task samples: look for concurrent writers, retried stages double-committing, permissions, and storage throttling.

The commands above are starting points, not limits: compose your own jq, call the REST API directly, or pull logs, code, and platform state when a question needs it. Rule each candidate in or out with evidence, and when a signal is suggestive but not conclusive, go a level deeper (more task samples, the exact log lines, the executed plan) until you are confident it is or is not the cause. Do not settle for the first plausible explanation.

Before concluding, merge the driver log, executor logs, and platform events into one timeline and take the earliest error that explains the rest. Configuration proves intent, not behavior; prefer runtime evidence.

## Report

State the root cause and confidence, the failure chain from it, the evidence for and against, the smallest fix or reproduction, and what evidence was missing. Say "confirmed" only when logs or a reproduction establish causality; otherwise "leading hypothesis". Do not change production configuration or rerun expensive jobs without approval.
