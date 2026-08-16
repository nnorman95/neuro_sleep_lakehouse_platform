# Process Optimization

Process optimization in NeuroSleep means reducing unnecessary work and manual
recovery while keeping lineage, validation, and failure behavior explicit.

The goal is not to add more tools. The goal is to make the same engineering work
repeatable, safer, easier to operate, and cheaper to rerun.

## 1. Optimization principles

The project follows these rules:

- do not repeat expensive work when a verified output already exists;
- do not process high-volume data when the downstream requirement does not use it;
- keep one canonical execution path for common operations;
- make retries and reruns idempotent;
- recover automatically only when the recovery boundary is unambiguous;
- fail closed when version or lineage selection is ambiguous;
- validate before publication and verify after publication;
- keep operational failures separate from data-quality failures;
- preserve enough lineage to explain exactly which source representation produced
  a downstream result;
- measure concrete reductions in files, rows, manual steps, or repeated work
  rather than claiming optimization without evidence.

## 2. Bronze

Bronze avoids repeated downloads and unsafe manual cleanup through:

```text
streaming download
official checksum verification
verified-object recovery
idempotent registration
retryable HTTP and object-storage operations
advisory locks
heartbeats
safe interruption cleanup
.part cleanup
storage/registry reconciliation
```

A verified immutable source object is reused instead of downloaded again.

## 3. Silver

Silver combines deterministic version identity with atomic publication:

```text
source_pair_id
input_fingerprint
config_id
schema_version
transform_version
_SUCCESS.json
```

A matching completed representation is skipped. An incomplete exact versioned
prefix can be removed and rebuilt without touching other valid representations.

Quality-gate failures are routed to durable quality history and quarantine
metadata, while runtime/network/storage/database failures remain operational
failures.

## 4. Requirement-driven processing

The relational analytical cohort contains 18 recordings, but only five currently
need sample-level signal processing.

The additional 13 recordings were processed in metadata-only mode because Phase 7
marts require recording/channel/epoch metadata, not raw signal samples.

This avoided generating and processing high-volume signal Parquet for data that
the relational analytical requirement did not use.

## 5. Warehouse selection

The Warehouse does not select an arbitrary "latest" Silver representation.

Current-state selection is fail-closed. If the compatible representation is
ambiguous, the build stops instead of silently choosing by load timestamp or UUID.

That turns a potentially manual data-correction problem into an explicit,
repeatable control.

## 6. Spark input optimization

The Silver bucket physically contained:

```text
1,731 signal Parquet files
```

The current Warehouse-selected representations require:

```text
1,416 signal Parquet files
116,242,840 signal rows
~0.698 GiB
```

The extra 315 files belong to an unselected historical SC4001E representation and
are excluded automatically through exact Warehouse/Silver-manifest lineage.

Spark therefore reads the current logical dataset rather than using a broad
bucket wildcard.

The current files are grouped by Spark into a much smaller number of input
partitions:

```text
SC4001E   315 files -> 11 partitions
SC4002E   336 files -> 12 partitions
SC4011E   329 files -> 12 partitions
SC4012E   336 files -> 12 partitions
ST7011J   100 files -> 10 partitions
```

No additional file-read tuning is applied without evidence that the defaults are
a bottleneck.

## 7. Feature compaction

Phase 8 changes the downstream grain from individual samples to 30-second
recording/channel windows:

```text
Silver input:
1,416 Parquet files
116,242,840 sample rows

Gold output:
5 Parquet files
83,909 feature rows
```

This is approximately 283 times fewer data files.

The row reduction is not deletion of the trusted sample dataset. Silver remains
available unchanged. Gold is a purpose-built downstream representation that
stores reusable window statistics instead of every source sample.

Current Gold Parquet occupies 4.328 MiB compared with roughly 0.698 GiB of the
selected Silver signal input.

## 8. Idempotent Gold publication

The first complete Gold build produced one compact Parquet object per selected
recording.

The immediate complete rerun produced:

```text
written=0
skipped=5
recovered_objects=0
```

No Spark feature recomputation is required for a recording whose exact completed
Gold representation already passes the publication contract.

Incomplete exact prefixes can be recovered automatically. Completed prefixes are
never auto-deleted merely because later validation fails.

## 9. Canonical commands

Common validation and operation paths are exposed through Make targets:

```text
make test
make spark-smoke
make spark-feature-check
make gold-signal-features
make gold-signal-features-check
make gold-reliability-smoke
make feature-integration-check
make integrated-signal-features
make integrated-signal-features-check
make integrated-gold-reliability-smoke
make phase8-check
make phase9-check
make airflow-bootstrap
make airflow-smoke
make phase10-check
```

This reduces the need to remember long combinations of Python module paths,
`PYTHONPATH`, Spark package arguments, and individual smoke scripts.

## 10. Phase 9 integration reuse

Phase 9 reuses the compact Phase 8 Gold feature layer instead of repeating the
sample-level Spark aggregation.

```text
Silver signal source:
116,242,840 sample rows

Phase 8 reusable Gold:
5 Parquet files
83,909 feature rows

Phase 9 integration input:
the same 5 compact Gold feature files
+ Warehouse context

Phase 9 output:
5 integrated Parquet files
83,909 rows
```

Signal feature calculation and relational label/context integration can change
independently. An unchanged source Gold publication and unchanged Warehouse
context produce an already-completed integrated prefix that is skipped.

The Warehouse context receives a deterministic SHA-256 fingerprint. If the
context changes, Phase 9 produces a new immutable representation rather than
requiring manual cleanup or overwriting historical output.


## 11. Phase 10 orchestration and manual-step reduction

Before Phase 10, a full pipeline run required remembering and executing the
Bronze, Silver, staging, dbt, Gold, and integration commands in the correct order.
Phase 10 replaces that manual dependency management with one Airflow DAG while
reusing the same implementation underneath.

```text
manual workflow:
multiple ordered commands + manual dependency tracking

Phase 10 workflow:
one neurosleep_lakehouse_pipeline DAG run
8 existing project tasks
bounded parallelism=2
max_active_runs=1
```

This removes orchestration duplication rather than moving business logic into
Airflow. The same idempotency, reconciliation, quality gates, and fail-closed
selection rules remain inside the project modules and scripts.

Two consecutive full DAG runs completed successfully. On the unchanged rerun,
the Gold feature task completed in about 6.5 seconds compared with about 135
seconds on the first run, consistent with the existing completed-publication skip
path. The full Phase 9 regression passed after the repeated Airflow runs.

The Airflow bootstrap is also rerunnable and now builds the project execution
image itself. A fresh environment therefore does not depend on a manually
prebuilt local image.

## 12. What is deliberately not optimized

The project does not currently add:

- a Spark cluster for a sub-gigabyte selected Parquet input;
- custom Spark file-partition settings without a measured bottleneck;
- raw signal rows in PostgreSQL;
- signal generation for the metadata-only analytical cohort;
- scientific feature families that have no current downstream requirement;
- destructive cleanup of historical valid Silver or Gold representations.

These omissions are part of the optimization strategy: operational simplicity is
preferred over infrastructure or tuning that does not solve a demonstrated
problem.

## 13. Evidence to preserve in later phases

Later phases should continue recording evidence such as:

```text
manual steps removed
rerun work skipped
input/output file counts
input/output row counts
runtime where comparisons are meaningful
recovery behavior
failure visibility
new operational dependencies
```

A process change should be considered an optimization only when it reduces real
work or risk without weakening correctness, lineage, or observability.
