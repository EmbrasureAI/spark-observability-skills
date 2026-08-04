---
name: debug-slow-spark-job
description: Diagnose slow, expensive, or regressed Apache Spark and PySpark applications by comparing runtime evidence against a healthy run. Use for long stages, stragglers, skew, shuffle, spill, garbage collection, poor parallelism, small files, slow scans, scheduler delay, executor imbalance, and unexplained compute-cost growth.
---

# Debug a slow Spark job

Find the root cause of the slowdown or cost growth. Compare against a healthy run whenever one exists, and normalize for input size before calling anything a regression.

## Get the evidence

```bash
export SPARK_HISTORY_URL="https://history.example.com"
python3 scripts/spark_history_api.py slow --app-id <slow-application-id> > /tmp/spark-slow.json
python3 scripts/spark_history_api.py slow --app-id <healthy-application-id> > /tmp/spark-healthy.json
```

Run from this skill directory. If `SPARK_HISTORY_URL` is unset, find the server before asking the user: try `http://localhost:18080`, a running application's UI on `http://localhost:4040`, and the history-server or eventLog settings in the local Spark config; ask only when nothing responds. Authentication comes from `SPARK_HISTORY_AUTHORIZATION`, `SPARK_HISTORY_COOKIE`, or `SPARK_HISTORY_HEADERS_JSON`; never ask for credentials in chat and never disable TLS verification (`--ca-file` for a private CA).

Other subcommands: `applications [--status completed|running]` to find app IDs, `sql-list --app-id <id>` and `sql --app-id <id> --execution-id <n>` for the executed plan with per-node metrics, `failure --app-id <id>` for failed-stage detail; all accept `--stage-limit N --task-limit N`. For anything the profiles omit, call `$SPARK_HISTORY_URL/api/v1` directly with the same auth headers: `/applications/{app}/jobs`, `/stages/{stage}/{attempt}/taskSummary?quantiles=0.05,0.5,0.95`, `/stages/{stage}/{attempt}/taskList?sortBy=-runtime`, `/allexecutors`, `/environment`, `/sql/{execution}?details=true&planDescription=true`.

In `taskSummary`, metric arrays align with `quantiles` `[0, 0.5, 0.95, 0.99, 1.0]`: the middle entry is the median, the last is the max. Read `spark.master` and deploy mode from `environment.sparkProperties` to know where driver and executor logs live. Compare stage timelines between the two runs and start where they first diverge; wall time alone mixes queue time, driver work, execution, and commit.

## Likeliest causes, in order, and how to check each

1. **Skew.** Max far above median for run time, input, or shuffle read in one stage:

   ```bash
   jq '.longestStages[] | {stage: .stage.stageId, quantiles: .taskSummary.quantiles, run: .taskSummary.executorRunTime, input: .taskSummary.inputMetrics.bytesRead, shuffleRead: .taskSummary.shuffleReadMetrics.readBytes}' /tmp/spark-slow.json
   ```

   `.longestStages[].tasks` names the hot partitions and hosts. Check the executed plan (`sql` subcommand) for AQE skew handling (`skewed=true`) before proposing salting.

2. **Wrong partition count.** Tasks uniformly large and spilling mean too few; tens of thousands of sub-second tasks mean scheduler overhead:

   ```bash
   jq '{stageTasks: [.longestStages[].stage | {id: .stageId, tasks: .numTasks}], totalCores: ([.allExecutors[] | select(.isActive) | .totalCores] | add), conf: [.environment.sparkProperties[] | select(.[0] | test("shuffle.partitions|executor.cores"))]}' /tmp/spark-slow.json
   ```

   Try `spark.sql.shuffle.partitions` or AQE targets before inserting explicit repartitions.

3. **Excess spill.** Spill without skew means operator state outgrew execution memory:

   ```bash
   jq '.longestStages[] | {stage: .stage.stageId, memSpill: .taskSummary.memoryBytesSpilled, diskSpill: .taskSummary.diskBytesSpilled}' /tmp/spark-slow.json
   ```

   Reduce state (narrower rows, partial aggregation) or raise partitions before raising memory.

4. **Large shuffles.** Shuffle bytes dominating stage runtime:

   ```bash
   jq '.longestStages[].stage | {id: .stageId, shuffleRead: .shuffleReadBytes, shuffleWrite: .shuffleWriteBytes, run: .executorRunTime}' /tmp/spark-slow.json
   ```

   Map the stage to its `Exchange` in the executed plan and ask whether the shuffle is avoidable: broadcast, pre-aggregation, or already-partitioned data.

5. **Slow shuffle fetch.** Low CPU with high fetch wait or heavy remote reads:

   ```bash
   jq '.longestStages[] | {stage: .stage.stageId, fetchWait: .taskSummary.shuffleReadMetrics.fetchWaitTime, remote: .taskSummary.shuffleReadMetrics.remoteBytesRead}' /tmp/spark-slow.json
   ```

   Dead entries in `.allExecutors[] | select(.isActive | not)` mean data was refetched or recomputed. If `environment` shows Celeborn or an external shuffle service, confirm from the driver log it actually served the shuffle before tuning it.

6. **GC pressure.** GC a large fraction of run time:

   ```bash
   jq '{stages: [.longestStages[] | {id: .stage.stageId, gc: .taskSummary.jvmGcTime, run: .taskSummary.executorRunTime}], executors: [.allExecutors[] | {id, gc: .totalGCTime, dur: .totalDuration}]}' /tmp/spark-slow.json
   ```

   Check peak memory, cache use, and object-heavy code before resizing heaps.

7. **Python UDF transport.** Time concentrated at Python boundaries in the plan:

   ```bash
   python3 scripts/spark_history_api.py sql --app-id <id> --execution-id <n> | jq '.sqlExecution.nodes[] | select(.nodeName | test("Python")) | {nodeName, metrics}'
   ```

   Prefer native expressions or vectorized UDFs over resource changes.

8. **Retry churn.** Failures that succeeded on retry inflate runtime without failing the job:

   ```bash
   jq '{stages: [.longestStages[].stage | select(.numFailedTasks > 0) | {id: .stageId, failed: .numFailedTasks}], jobs: [.jobs[] | select(.numFailedTasks > 0) | {jobId, failed: .numFailedTasks}]}' /tmp/spark-slow.json
   ```

   Find the flaky cause (one bad host, preemption, timeouts) in the executor log or node events.

9. **Scan overhead.** Long scans with little output:

   ```bash
   python3 scripts/spark_history_api.py sql --app-id <id> --execution-id <n> | jq '.sqlExecution.nodes[] | select(.nodeName | startswith("Scan")) | {nodeName, metrics}'
   ```

   Too many small files, missing partition or pushed filters, or slow storage.

10. **Driver and queue time.** Gaps before the first task or between jobs:

    ```bash
    jq '[.jobs[] | {jobId, submissionTime, completionTime}] | sort_by(.submissionTime)' /tmp/spark-slow.json
    ```

    That time is planning, file listing, queueing, or provisioning: check the driver log for what it was doing, not stage tuning.

The commands above are starting points, not limits: compose your own jq, call the REST API directly, or pull logs, code, and platform state when a question needs it. Rule each candidate in or out with evidence, and when a signal is suggestive but not conclusive, go a level deeper (more task samples, the exact log lines, the executed plan) until you are confident it is or is not the cause. Do not settle for the first plausible explanation.

For a regression, end by naming what changed: code, data volume or distribution, configuration (diff the two snapshots' `environment`), or infrastructure. Adding memory for a skewed partition, adding executors when task count caps parallelism, and caching once-used data are common wrong answers.

## Report

State the root cause and confidence, the first divergent stage, the evidence for and against, alternatives you rejected, and the most likely fix. Do not change production settings or launch expensive reruns without approval.
