# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Building and Running
- `go build -v -race` - Build the logferry binary with race detection
- `go build -v -race` (in logferry-api/) - Build the logferry-api binary with race detection
- `./run` - Run logferry client in development mode
- `./logferry-api/run` - Run logferry-api server in development mode
- `gofumpt -w .` - Format all Go files (always run before commits)

### Development Commands
- `go mod tidy` - Clean up dependencies
- `make sign` - Sign with drone CI (production)

## Architecture

### Core Components
- **logferry**: Main client that watches for Avro log files and uploads them to the API server
- **logferry-api**: HTTP API server that receives log data and forwards to Kafka

### Key Modules
- **main.go**: Client entry point with file watching, certificate management, and upload coordination
- **uploader.go**: Core upload functionality with retry logic and file processing
- **http.go**: HTTP client management with TLS and connection pooling
- **cleaner.go**: Directory cleanup for processed files
- **logferry-api/**: Server implementation with Echo framework, Kafka integration, and Avro processing

### Data Flow
1. Client watches directory for new .avro files
2. Files are processed and uploaded via HTTPS with client certificates
3. API server receives uploads and forwards to Kafka
4. Processed files are cleaned up after successful upload

### Configuration
- Client uses command-line flags: `-cert`, `-key`, `-path`, `-upstream`
- API server uses environment variables: `TLS_CERT`, `TLS_KEY`, `KAFKA_*`
- Default config in `scripts/defaults`

### TLS & Security
- Mutual TLS authentication between client and server
- Client certificates managed via apitls.GetCertman()
- Kafka connections use separate TLS certificates
- OpenTelemetry tracing integrated throughout

### Dependencies
- Uses common ntppool.org libraries for TLS, logging, metrics, and tracing
- Kafka integration via segmentio/kafka-go
- Avro processing via hamba/avro
- Echo framework for HTTP API
- Prometheus metrics on port 9097 (client) and via Echo (server)