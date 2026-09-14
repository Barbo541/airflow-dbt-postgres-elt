# Performance and single-node limits

## Policy

Performance changes are evidence-driven. The production fixture contains only
522 users, 940 health rows, and 938 activity rows, so a more complex load path is
not automatically an improvement. The current design intentionally keeps Pandas
and SQLAlchemy `to_sql()` until measurement shows a material bottleneck.

The same rule applies to PostgreSQL indexes: a full-scan/hash-join plan can be
faster than an index-driven plan for small relations and full-refresh builds.

## Reproducible benchmark

From the repository root, with Docker available:

```sh
python scripts/benchmark.py --scales 1 100 --output .validation/benchmark.json
```

An optional stress boundary can be run explicitly:

```sh
python scripts/benchmark.py --scales 1 100 1000 \
  --output .validation/benchmark-stress.json
```

The benchmark does **not** use the developer Compose project. It creates one
random `pet-benchmark-*` project, generated credentials, temporary source files,
and disposable volumes. It rewrites only the benchmark bind-mounted source
directory between scale runs and removes only its own project at the end.

Scale `N` keeps the 522-row user dimension and repeats the health/activity event
fixtures `N` times. Source-row ordinals remain unique because they are assigned
after the scaled CSV is read.

## Measurements produced

For each scale the harness reports:

- input/final event rows;
- `load_raw_data` Airflow task duration;
- `dbt_build` Airflow task duration;
- trigger-to-success DAG wall time;
- top PostgreSQL plan node for the final enrichment query;
- PostgreSQL planning and execution time from `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`.

The JSON output is ignored by Git by default so hardware-specific local results do
not become accidental universal claims.

## Recorded final benchmark

The final release candidate was measured locally on 2026-09-13 using Windows as
the host and Docker Desktop with Linux containers. The runtime under test used
Airflow `3.3.1`, PostgreSQL `15.19`, dbt Core `1.11.11`, and dbt-postgres
`1.11.0`. Host CPU/RAM and Docker resource allocation were not captured, so the
results are evidence for relative behavior on this machine, not a portable SLA.

| Scale | Events | Load task (s) | dbt task (s) | DAG wall (s) | Plan | Planning (ms) | Execution (ms) |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 1x | 1,878 | 7.381 | 9.079 | 27.938 | Hash Join | 0.744 | 1.557 |
| 100x | 187,800 | 7.536 | 13.018 | 33.047 | Hash Join | 0.708 | 139.979 |
| 1000x | 1,878,000 | 66.020 | 57.655 | 133.796 | Hash Join | 1.237 | 1,472.926 |

### Interpretation

- The real fixture is tiny relative to the stress cases, so platform orchestration
  overhead dominates the normal run.
- At 100x, 187,800 events still complete in about 33 seconds end to end and the
  final query executes in about 140 ms.
- At 1000x, 1,878,000 events complete successfully in about 134 seconds. At this
  artificial boundary, both ingestion and dbt transformation become material
  portions of total runtime.
- PostgreSQL chose the same `Hash Join` at all tested scales. There is no measured
  evidence that an additional join index is required for this full-refresh query.
- The benchmark does not isolate `to_sql()` from CSV parsing, validation, hashing,
  DataFrame preparation, and task overhead. Therefore the 66-second 1000x load
  task is not evidence that `COPY` alone would save 66 seconds.

### v1.0 performance decision

For the stated snapshot workload, keep Pandas/SQLAlchemy `to_sql()`, the current
chunking, full-refresh semantics, dbt views, and PostgreSQL's selected join plan.
Do not add `COPY` or extra indexes solely for portfolio complexity. Re-evaluate
`COPY` only if future relevant volumes make `load_raw_data` a measured bottleneck
or a controlled before/after test demonstrates a material end-to-end gain.

## Decision rules

### Keep `to_sql()` when

- ingestion is a small fraction of total DAG time;
- the default fixture completes comfortably within the operational timeout;
- scale growth is acceptable for the stated local/snapshot scope;
- replacing it with `COPY` would add complexity without a meaningful end-to-end gain.

### Evaluate PostgreSQL `COPY` when

- `load_raw_data` dominates DAG time at relevant scale;
- growth is clearly superlinear or insert round trips become the measured limit;
- a controlled comparison shows a material end-to-end improvement, not only a
  microbenchmark improvement.

### Add an index only when

- `EXPLAIN (ANALYZE, BUFFERS)` identifies a repeated read bottleneck;
- the indexed plan improves the measured workload enough to offset write/maintenance cost;
- the index supports an actual query/constraint requirement rather than a generic rule.

For the final enrichment build, a sequential scan plus hash join can be the
correct plan because both sides are read almost completely.

## Full refresh versus incremental

The source contract is a complete snapshot. `TRUNCATE + reload` inside one
transaction therefore provides simple, deterministic semantics and strong
rollback behavior. Incremental/UPSERT/CDC logic would be justified only if the
source contract changed to append/change events or the measured full-refresh cost
became unacceptable.

## Reporting results in the portfolio

Only publish timings measured on an identified environment. Record at least:

```text
Host/OS:
Docker version:
CPU/RAM allocated to Docker:
Airflow image:
PostgreSQL image:
dbt versions:
Scales tested:
```

Avoid claims such as "10x faster" unless the repository includes the before/after
method and both measurements. A valid outcome is also **no optimization added**
when measurement shows the simpler implementation is already sufficient.
