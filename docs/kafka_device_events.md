# Kafka Device Events

Phase 11 adds a local Kafka device-event path alongside the existing
PhysioNet batch/lakehouse path. It does not replace Bronze, Silver, Gold,
Airflow, or the existing relational analytics flow.

## 1. Scope

Implemented source:

```text
source_system: simulated_bci_device
schema_version: 1.0.0
topic: neurosleep.simulated-bci.device-events.v1
partitions: 3
replication_factor: 1
cleanup.policy: delete
retention.ms: 604800000
```

The producer emits:

```text
session_started
signal_quality
battery_status
session_ended
```

Each event contains a stable `event_id`, `device_id`, `session_id`,
UTC `event_time`, non-negative `sequence_number`, and event-specific payload.

## 2. Runtime

The local Kafka runtime uses Apache Kafka 4.3.1 in single-node KRaft mode.

```text
host bootstrap:      localhost:9092
Docker bootstrap:    kafka:19092
controller:          kafka:29093
auto topic creation: false
persistent volume:   kafka_data
```

ZooKeeper is not used.

Useful commands:

```bash
make kafka-up
make kafka-down
make kafka-ps
make kafka-smoke
make kafka-init
make kafka-topic-check
```

## 3. Producer

The simulated BCI producer:

- validates every event against the versioned event contract;
- uses `device_id` as the Kafka message key;
- keeps one device on one Kafka partition under the configured partitioner;
- uses idempotent producer settings;
- requires `acks=all`;
- disables automatic topic creation;
- writes source `event_time` as the Kafka timestamp;
- includes `schema_version` and `event_type` headers.

Producer transport idempotence is not treated as business-level deduplication.

## 4. Consumer Validation

The consumer validates:

```text
topic
partition
offset
UTF-8 key/value
JSON structure
device-event contract
Kafka key == event.device_id
required headers
duplicate headers
Kafka timestamp == event_time milliseconds
```

The consumer explicitly disables:

```text
enable.auto.commit
enable.auto.offset.store
```

Kafka offsets move only after a durable outcome has been recorded.

## 5. Durable Inbox

Valid events are written to:

```text
ops.kafka_device_event_inbox
```

Grain:

```text
one stable event_id
```

`event_id` is the business deduplication key.

The inbox also stores:

```text
canonical event fingerprint
first Kafka partition/offset
last Kafka partition/offset
Kafka timestamp
Kafka headers
first_ingested_at
last_ingested_at
delivery_count
```

An identical replay increments delivery metadata without creating a second
event row. Reuse of one `event_id` with different canonical event content fails
closed.

## 6. Delivery Semantics

The implemented processing order is:

```text
Kafka message
    |
    v
validate
    |
    v
durable PostgreSQL outcome
    |
    v
synchronous Kafka offset commit(offset + 1)
```

The project therefore implements practical **at-least-once processing** with
idempotent database effects.

It does not claim distributed exactly-once semantics.

Failure behavior:

```text
failure before durable write
    -> no Kafka offset commit
    -> event is replayed

durable write succeeds but Kafka offset commit does not
    -> event may be replayed
    -> event_id dedup keeps the database effect idempotent
```

## 7. Invalid Messages

Contract-invalid Kafka messages do not stop the whole stream.

They are routed to the existing:

```text
quality.quarantine_records
```

Stable quarantine identity:

```text
source_system
+ kafka://topic/partition/offset record_key
+ error_code
```

The raw transport payload, key, headers, timestamp, topic, partition, and offset
are preserved in the quarantine payload representation.

The Kafka offset is committed only after quarantine persistence succeeds.
If quarantine persistence fails, the offset remains unchanged.

## 8. Late and Out-of-Order Events

Structurally valid late or out-of-order events remain valid events. They are not
quarantined.

The durable inbox records arrival classification:

```text
arrival_classification_version
ingestion_delay_ms
late_threshold_ms
is_late
is_out_of_order
out_of_order_reason
previous_max_sequence_number
previous_max_event_time
```

Current local policy:

```text
late threshold: 60,000 ms
```

Out-of-order classification compares first arrival against already persisted
events from the same device session.

A forward sequence gap by itself is allowed:

```text
0 -> 2
```

A later backward arrival can be marked out of order:

```text
0 -> 2 -> 1
```

The reason can be:

```text
sequence
event_time
sequence_and_event_time
```

## 9. Warehouse Fact

Trusted inbox events are exposed through dbt as:

```text
warehouse.fact_device_event
```

Grain:

```text
one validated source event_id
```

The model retains:

- deterministic Warehouse `device_event_sk`;
- source event identity and payload;
- device/session identity;
- event time and sequence;
- event fingerprint;
- late/out-of-order classification;
- durable ingestion timestamps;
- Kafka lineage and delivery metadata.

The dbt model uses an enforced contract, source tests, uniqueness checks,
relationship tests, and deterministic full-table rebuild behavior.

The model is registered in governance and all 27 physical columns are classified.

## 10. Validation

Focused validators:

```bash
make kafka-topic-check
make kafka-producer-check
make kafka-consumer-check
make kafka-inbox-check
make kafka-ingestion-check
make kafka-invalid-check
make kafka-arrival-check
make kafka-warehouse-check
```

Complete Phase 11 audit:

```bash
make phase11-check
```

The final Phase 11 audit verifies:

```text
event contract
Kafka runtime
topic contract and idempotent initialization
producer delivery
consumer isolation
event_id deduplication
durable ingestion
restart-safe offset handling
invalid-message quarantine
late-event detection
out-of-order detection
warehouse.fact_device_event
```

Final verified Phase 11 status:

```text
phase11_validation_status=success
```

The Phase 10 regression was also rerun after Phase 11 and remained green,
including the complete Phase 9 regression and a 301/301 dbt build.
