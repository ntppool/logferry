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
- Runs in Kubernetes with TLS termination
- Provides health checks and Prometheus metrics via Echo framework
- Uses mutual TLS for client authentication

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

## TLS Certificate Management

Both components require TLS certificates for secure communication:

- **Client certificates**: Used for mutual TLS authentication with the API server
- **Server certificates**: Used by the API server for HTTPS
- **Kafka certificates**: Used by the API server to connect to Kafka

Certificate management is handled externally (e.g., using HashiCorp Vault, cert-manager, or manual certificate deployment).

## Monitoring

### Prometheus Metrics

#### Client (logferry)
- Metrics available on port 9097
- Includes process metrics, upload timing, and file processing statistics

#### API Server (logferry-api)  
- Metrics integrated with Echo framework endpoints
- Includes HTTP request metrics, Kafka publishing metrics, and health status

### Health Checks

The API server provides health check endpoints for Kubernetes probes.

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