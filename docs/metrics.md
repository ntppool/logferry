# Prometheus Metrics Reference

Both logferry (client) and logferry-api (server) export Prometheus metrics.

## Client (logferry) - port 9097

### File processing

| Metric | Type | Description |
|--------|------|-------------|
| `queue_internal_count` | gauge | Files in the in-memory channel waiting to be processed |
| `queue_disk_count` | gauge | `.avro` files on disk in the watch directory |

### Uploads

| Metric | Type | Description |
|--------|------|-------------|
| `upload_success_total` | counter | Successful file uploads |
| `upload_failure_total` | counter | Failed file uploads |
| `upload_duration_seconds` | histogram | Upload latency for successful requests (buckets: 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 5, 10 seconds) |

### Certificate

| Metric | Type | Description |
|--------|------|-------------|
| `client_cert_expiry_timestamp_seconds` | gauge | Unix timestamp of client certificate NotAfter |
| `client_cert_expired` | gauge | 1 if client certificate is expired, 0 if valid |

### SRV target pool

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `logferry_srv_target_requests_total` | counter | `target` | Requests sent to each SRV target |
| `logferry_srv_target_errors_total` | counter | `target` | Errors per SRV target |
| `logferry_srv_target_healthy` | gauge | `target` | 1 if target is healthy, 0 if in backoff |
| `logferry_srv_targets_total` | gauge | | Current number of SRV targets (0 in fallback mode) |
| `logferry_srv_lookups_total` | counter | | SRV DNS lookups performed |
| `logferry_srv_fallback_active` | gauge | | 1 if using direct `--upstream` (no SRV) |

## Server (logferry-api) - port 4281

### HTTP (echoprometheus, subsystem "logferry")

These are registered by the Echo `echoprometheus` middleware with the `logferry` subsystem prefix.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `logferry_request_duration_seconds` | histogram | `method`, `status`, `url` | Request latency (buckets: 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 16 seconds) |
| `logferry_request_size_bytes` | histogram | `method`, `status`, `url` | Request body size (buckets: 0, 1KB, 16KB, 128KB, 512KB, 1MB, 4MB, 8MB, 16MB) |
| `logferry_response_size_bytes` | histogram | `method`, `status`, `url` | Response body size (same buckets as request size) |
| `logferry_requests_total` | counter | `method`, `status`, `url` | Total HTTP requests |

### Kafka writer (subsystem "logferry_kafka")

These track the segmentio/kafka-go writer stats, polled every second.

| Metric | Type | Description |
|--------|------|-------------|
| `logferry_kafka_messages` | counter | Total Kafka messages written |
| `logferry_kafka_errors` | counter | Total Kafka write errors |
| `logferry_kafka_batch_time` | summary | Time spent building batches (seconds) |
| `logferry_kafka_batch_bytes` | summary | Batch sizes (bytes) |
| `logferry_kafka_batch_queue_time` | summary | Time batches spend queued (seconds) |
| `logferry_kafka_write_time` | summary | Time spent writing to Kafka (seconds) |
| `logferry_kafka_wait_time` | summary | Time spent waiting for Kafka responses (seconds) |
