# Clickstream Lakehouse — Execution & Validation Guide

## 1. Overview

This guide explains how to start the local Docker infrastructure, initialize the MinIO Bronze bucket, run the clickstream event generator and ingestion consumer, validate the ingestion results, and clean up the environment.

### Components

- **Apache Kafka:** Streams clickstream events.
- **MinIO:** Provides S3-compatible object storage for Bronze data.
- **Generator:** Produces synthetic clickstream events.
- **Ingestion Consumer:** Consumes Kafka messages and writes valid events to partitioned storage while routing malformed events to a Dead-Letter Queue (DLQ).

## 2. Prerequisites

Ensure the following are installed:

- Docker and Docker Compose
- Python 3
- `pip`

## 3. Start the Infrastructure

From the project directory, start the Docker services:

```bash
docker compose up -d
```

Verify that Kafka and MinIO are running:

```bash
docker ps
```

Confirm that `clickstream-kafka` and `clickstream-minio` appear in the running containers list.

## 4. Initialize the MinIO Bucket

1. Open the MinIO Web UI: [http://localhost:9001](http://localhost:9001)
2. Log in using the configured credentials:
   - **Username:** `admin`
   - **Password:** `password123`
3. Create a bucket named:

```text
bronze-clickstream
```

This bucket serves as the Bronze storage layer for ingested clickstream events.

> **Note:** These credentials must match the MinIO configuration in your Docker Compose file.

## 5. Install Python Dependencies

Install the required Python packages:

```bash
python -m pip install kafka-python boto3 faker
```

| Package        | Purpose                                         |
| -------------- | ----------------------------------------------- |
| `kafka-python` | Produces and consumes Kafka messages            |
| `boto3`        | Connects to MinIO through its S3-compatible API |
| `faker`        | Generates synthetic clickstream data            |

## 6. Run the Pipeline

Open two separate terminals in the project directory.

### Terminal 1 — Start the Event Generator

```bash
python generator.py
```

The generator creates synthetic clickstream events and publishes them to Kafka.

### Terminal 2 — Start the Bronze Ingestion Consumer

```bash
python ingestion.py
```

The consumer reads events from Kafka, processes them, and writes the results to MinIO.

## 7. Execution & Validation

### A. Validate Infrastructure

Run:

```bash
docker ps
```

**Expected result:** Both `clickstream-kafka` and `clickstream-minio` are running.

### B. Validate Bronze Ingestion

Monitor the ingestion consumer's console output.

Verify that:

- Valid events are successfully written to the Bronze storage layer.
- Malformed events are routed to the Dead-Letter Queue.
- The output paths follow the expected partition structure.

Example logical directory layout:

```text
bronze-clickstream/
├── events/
│   └── year=YYYY/
│       └── ...
└── dlq/
    └── year=YYYY/
        └── ...
```

The exact directory depth depends on the partitioning logic implemented in `ingestion.py`.

### C. Validate Physical Partitioning

Open the MinIO Web UI at [http://localhost:9001](http://localhost:9001) and inspect the `bronze-clickstream` bucket.

Confirm that:

- Valid event objects exist under the `events/` prefix.
- Malformed event objects exist under the `dlq/` prefix.
- Partition paths match the UTC event timestamps used by the ingestion logic.
- Events are assigned to the correct historical UTC time partitions, rather than being partitioned solely by ingestion time.

> **Important:** S3-compatible object storage uses object keys and prefixes rather than actual directories. The folder-like structure shown in MinIO is created by the object naming convention.

## 8. Stop and Clean Up

Stop the generator and ingestion scripts using `Ctrl+C` in their respective terminals.

Then stop the Docker services and remove associated Compose-managed volumes:

```bash
docker compose down -v
```

**Warning:** The `-v` flag removes Compose-managed volumes and may permanently delete locally persisted Kafka or MinIO data. Use it only when you want a clean testing environment.

