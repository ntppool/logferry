# Logferry

Logferry is a high-performance log shipping system designed to transport Avro-formatted log files from GeoDNS servers to Kafka. It consists of two components: a client that watches for log files and uploads them, and an API server that receives uploads and forwards them to Kafka.

## Architecture

```
[GeoDNS] → [Avro Files] → [logferry client] → [HTTPS/TLS] → [logferry-api] → [Kafka]
```

The system is designed for the [NTP Pool project](https://www.ntppool.org) and integrates with [GeoDNS](https://github.com/abh/geodns) to ship DNS query logs for analysis and monitoring.

## Components

### logferry (Client)
- Watches a directory for new `.avro` files created by GeoDNS
- Uploads files to the API server using mutual TLS authentication
- Automatically cleans up files after successful upload
- Provides Prometheus metrics on port 9097
- Runs as a native binary on Linux and FreeBSD

### logferry-api (Server)
- HTTP API server that receives log uploads from clients
- Forwards received data to Kafka topic `geodns-avro`
- Runs in Kubernetes (requires TLS passthrough, not termination)
- Provides health checks and Prometheus metrics via Echo framework
- Uses mutual TLS (mTLS) for client authentication

## Performance

The system has been tested by the NTP Pool project handling over **1 million log entries per second**.

## Installation

### Client (logferry)

1. Download or build the `logferry` binary for your platform
2. Install as a system service (example systemd service file in `scripts/logferry.service`)

### API Server (logferry-api)

Deploy in Kubernetes with appropriate TLS certificates and Kafka connectivity.

## Configuration

### GeoDNS Configuration

Configure GeoDNS to output Avro logs:

```toml
[avrolog]
path = log/avro/
maxsize = 500000
maxtime = 5s
```

This creates Avro files in the `log/avro/` directory that logferry will monitor and upload.

### Client Configuration

Run logferry with required parameters:

```bash
./logferry \
  -cert /path/to/client.crt \
  -key /path/to/client.key \
  -path /path/to/log/avro \
  -upstream https://logferry-api.example.com
```

#### Required Parameters

- `-cert`: Path to client TLS certificate
- `-key`: Path to client TLS private key  
- `-path`: Directory containing Avro log files (must match GeoDNS avrolog.path)
- `-upstream`: URL of the logferry-api server

#### Optional Parameters

- `-jsondump`: Dump specified Avro files to JSON format (for debugging)
- `-version`: Show version information

### API Server Configuration

The API server is configured via environment variables:

#### TLS Configuration
- `TLS_CERT`: Path to server TLS certificate
- `TLS_KEY`: Path to server TLS private key

#### Kafka Configuration
- `KAFKA_BROKERS`: Comma-separated list of Kafka broker addresses
- `KAFKA_TLS_CERT`: Path to Kafka client certificate
- `KAFKA_TLS_KEY`: Path to Kafka client private key
- `KAFKA_TLS_CA`: Path to Kafka CA certificate

Logs are published to the Kafka topic `geodns-avro` (currently not configurable).

## Authentication

Logferry uses mutual TLS (mTLS) for authentication. Both the client and server present certificates during the TLS handshake, and the server extracts the client's identity from its certificate.

### How It Works

1. **Client** presents its certificate during TLS handshake
2. **Server** verifies the certificate chain against its CA pool
3. **Server** extracts the DNS name from the client certificate
4. **Server** validates the DNS name ends with `.mon.ntppool.dev` or `.ntppool.net`
5. **Server** uses this hostname as the client identity for logging and Kafka records

If certificate validation fails or no valid hostname is found, the server returns HTTP 401 Unauthorized.

### TLS Requirements

| Component | Minimum TLS Version |
|-----------|---------------------|
| Client    | TLS 1.3             |
| Server    | TLS 1.2             |

### Certificate Requirements

- **Client certificates**: Must have a DNS name (SAN) ending in `.mon.ntppool.dev` or `.ntppool.net`
- **Server certificates**: Standard TLS server certificate
- **Kafka certificates**: Separate certificates for Kafka broker authentication

Certificate management is handled externally (e.g., using HashiCorp Vault, cert-manager, or manual certificate deployment).

### Kubernetes Ingress Compatibility

**Important**: The API server is NOT compatible with ingress controllers that terminate TLS.

The server requires direct access to the client's TLS certificate to:
- Verify the certificate chain
- Extract the DNS hostname for client identity
- Pass the hostname to Kafka records

If an ingress terminates TLS, the client certificate information is lost and authentication will fail.

#### Supported Deployment Options

1. **TLS Passthrough** (recommended for ingress)

   Configure your ingress for SSL passthrough so encrypted traffic routes directly to the pod:

   ```yaml
   # NGINX Ingress example
   apiVersion: networking.k8s.io/v1
   kind: Ingress
   metadata:
     name: logferry-api
     annotations:
       nginx.ingress.kubernetes.io/ssl-passthrough: "true"
       nginx.ingress.kubernetes.io/backend-protocol: "HTTPS"
   spec:
     rules:
     - host: logferry-api.example.com
       http:
         paths:
         - path: /
           pathType: Prefix
           backend:
             service:
               name: logferry-api
               port:
                 number: 8080
   ```

2. **LoadBalancer Service** (bypasses ingress)

   Expose the service directly via a cloud load balancer with TCP passthrough:

   ```yaml
   apiVersion: v1
   kind: Service
   metadata:
     name: logferry-api
   spec:
     type: LoadBalancer
     ports:
     - port: 443
       targetPort: 8080
       protocol: TCP
     selector:
       app: logferry-api
   ```

## Monitoring

### Prometheus Metrics

#### Client (logferry)

Metrics are available on port 9097. Each client runs its own metrics endpoint, so per-host visibility comes from the Prometheus scrape target's `instance` label.

| Metric | Type | Description |
|--------|------|-------------|
| `upload_success_total` | Counter | Files uploaded successfully |
| `upload_failure_total` | Counter | File upload failures |
| `upload_duration_seconds` | Histogram | Upload latency for successful requests |
| `queue_internal_count` | Gauge | Files in the in-memory queue waiting to be processed |
| `queue_disk_count` | Gauge | Avro files on disk waiting to be processed |

**SRV discovery metrics** (when using SRV-based upstream resolution):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `logferry_srv_target_requests_total` | Counter | `target` | Requests sent to each SRV target |
| `logferry_srv_target_errors_total` | Counter | `target` | Errors per SRV target |
| `logferry_srv_target_healthy` | Gauge | `target` | 1 if target is healthy, 0 if in backoff |
| `logferry_srv_targets_total` | Gauge | | Current number of SRV targets (0 if in fallback mode) |
| `logferry_srv_lookups_total` | Counter | | SRV DNS lookups performed |
| `logferry_srv_fallback_active` | Gauge | | 1 if using direct `--upstream` URL (no SRV) |

Standard Go process metrics (`process_*`) are also registered.

#### API Server (logferry-api)

Metrics are available on port 4281.

**HTTP request metrics** (via Echo middleware, labels: `code`, `method`, `url`):

| Metric | Type | Description |
|--------|------|-------------|
| `logferry_request_duration_seconds` | Histogram | Request duration per endpoint |
| `logferry_request_size_bytes` | Histogram | Request body size |
| `logferry_response_size_bytes` | Histogram | Response body size |
| `logferry_requests_total` | Counter | Total requests per endpoint and status code |

**Kafka writer metrics** (prefix `logferry_kafka_`):

| Metric | Type | Description |
|--------|------|-------------|
| `logferry_kafka_messages` | Counter | Total Kafka messages written |
| `logferry_kafka_errors` | Counter | Total Kafka write errors |
| `logferry_kafka_batch_time` | Summary | Time spent forming batches |
| `logferry_kafka_batch_bytes` | Summary | Batch size in bytes |
| `logferry_kafka_batch_queue_time` | Summary | Time batches spend queued |
| `logferry_kafka_write_time` | Summary | Time spent writing to Kafka |
| `logferry_kafka_wait_time` | Summary | Time spent waiting for Kafka |

Note: The server does not include a per-client-host label on its metrics. Client identity (extracted from mTLS certificates) is included in structured logs and OpenTelemetry traces but not in Prometheus metrics.

### Health Checks

The API server provides a health check endpoint on port 4280 for Kubernetes probes. It returns 200 if Kafka partitions are reachable and 502 otherwise.

## File Lifecycle

1. GeoDNS writes Avro log files to the configured directory
2. logferry detects new `.avro` files via filesystem watching
3. Files are uploaded to the API server using HTTPS with mutual TLS
4. API server validates and forwards data to Kafka
5. Upon successful Kafka publishing, logferry deletes the local Avro file
6. Failed uploads are retried with exponential backoff

## Building

### Prerequisites
- Go 1.23.5 or later
- Access to go.ntppool.org/common (private module)

### Build Commands

```bash
# Build client
go build -v -race

# Build API server  
cd logferry-api/
go build -v -race
```

### Development

```bash
# Run client in development mode
./run

# Run API server in development mode  
./logferry-api/run

# Format code (required before commits)
gofumpt -w .

# Run tests
go test ./...
```

## Deployment Example

### Systemd Service (Client)

```ini
[Unit]
Description=Logferry log shipper
After=network.target

[Service]
Type=simple
User=logferry
ExecStart=/usr/local/bin/logferry \
  -cert /etc/logferry/client.crt \
  -key /etc/logferry/client.key \
  -path /var/log/geodns/avro \
  -upstream https://logferry-api.ntppool.org
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Kubernetes Deployment (API Server)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: logferry-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: logferry-api
  template:
    metadata:
      labels:
        app: logferry-api
    spec:
      containers:
      - name: logferry-api
        image: logferry-api:latest
        ports:
        - containerPort: 8080
        env:
        - name: TLS_CERT
          value: /etc/tls/tls.crt
        - name: TLS_KEY  
          value: /etc/tls/tls.key
        - name: KAFKA_BROKERS
          value: kafka-broker-1:9092,kafka-broker-2:9092
        volumeMounts:
        - name: tls-certs
          mountPath: /etc/tls
        - name: kafka-certs
          mountPath: /etc/kafka-tls
      volumes:
      - name: tls-certs
        secret:
          secretName: logferry-api-tls
      - name: kafka-certs
        secret:
          secretName: kafka-client-certs
```

## Troubleshooting

### Common Issues

- **Certificate errors**: Verify certificate paths and permissions
- **Connection refused**: Check network connectivity and firewall rules
- **Kafka publishing failures**: Verify Kafka broker connectivity and authentication
- **File permission errors**: Ensure logferry user has read access to Avro directory

### Debug Commands

```bash
# Test Avro file parsing
./logferry -jsondump /path/to/file.avro

# Check version
./logferry -version

# View detailed logs
journalctl -u logferry -f
```

## License

This project is part of the NTP Pool infrastructure.