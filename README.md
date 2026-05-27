# data-platform-service

Data platform for streaming events into **Apache Iceberg** tables registered in the **AWS Glue Data Catalog**, with data files stored in **S3**.

### Iceberg, Glue, S3 buckets, and metadata

1. **Schema definitions** — Under [`catalog-data/`](catalog-data/), each subfolder is a Glue **namespace** (for example `global`, `fraud-detection`). Each `*.json` file describes one event type: Iceberg **table name**, S3 **bucket suffix** (`bucketName`), partition strategy, and column fields.

2. **S3 buckets (Terraform)** — [`infrastructure/terraform/events-data-bucket.tf`](infrastructure/terraform/events-data-bucket.tf) scans `catalog-data` and creates one bucket per schema, named `{namespace}-{bucketName}` (for example `global-api-events`). Bucket policies allow the Glue catalog IAM role to read and write objects.

3. **Glue catalog and Iceberg table metadata** — The **infrastructure** module (`AWSGlueCatalogServiceImpl`, invoked from `CatalogSchemaHandler` Lambda or locally) reads the same JSON files and uses Iceberg’s **GlueCatalog** with **S3FileIO**. It creates the Glue database (namespace) if needed, then creates the Iceberg table and initial metadata under `s3://{namespace}-{eventName}/` so engines such as Athena or QuickSight can discover the table.

4. **Runtime data and commits** — **events-processor** stages Parquet files under the table location, then **appends** new data files to the Iceberg table (snapshot metadata) when commit events are processed on the `dp-commit-raw-data-events` Kafka topic.

Provision buckets with Terraform first, apply catalog schemas, then run the processor so file writes and metadata commits stay aligned with the Glue catalog.

---

Maven multi-module project for the data platform event pipeline.

## Modules

| Module | Role |
|--------|------|
| **events-processor** | Main Micronaut application: Kafka Streams topology (ingest and group raw events), Kafka consumers, and persistence logic. Run this for normal processing. |
| **events-producer** | Companion Micronaut app for local testing: HTTP API that publishes sample events to Kafka (not required for production). |
| **infrastructure** | Shaded JAR for catalog setup: applies `catalog-data` schemas to AWS Glue / Iceberg and is deployed via Terraform (S3 buckets, IAM, Lambda). |

## Prerequisites

- **JDK 21** (matches `maven.compiler.source` / `target` in the root POM).
- **Apache Maven** 3.9+.
- **Apache Kafka** reachable at `localhost:9092` (or override bootstrap servers in configuration). Install a Kafka distribution and use its `bin/` scripts, or set `KAFKA_HOME` and prefix paths as `./bin/...` from that directory.

## Build

From the repository root:

```bash
mvn clean verify
```

Build a single module:

```bash
mvn clean package -pl events-processor
mvn clean package -pl events-producer
```

Runnable JARs are produced under each module’s `target/` directory (Micronaut parent packaging).

## Run

Start **Zookeeper** (if your Kafka version needs it) and **Kafka** before the apps.

**events-processor** (port **8082** by default):

```bash
cd events-processor && mvn mn:run
```

Or from the root:

```bash
mvn -pl events-processor mn:run
```

**events-producer** (port **8081**, optional for load-style tests):

```bash
mvn -pl events-producer mn:run
```

Example producer HTTP call (see `events-producer` `DataResource`):

```text
GET http://localhost:8081/events/generate?count=5
```

Note: the test producer’s `DPEventClient` publishes to the topic **`dp-app-events`**. The processor’s stream **reads `dp-raw-events`**. For end-to-end tests that match the processor topology, either publish to `dp-raw-events` (for example with the console producer below) or align topics in code if you wire `dp-app-events` into the same pipeline.

## Configuration

### events-processor

Primary file: `events-processor/src/main/resources/application.yml`.

| Area | Purpose |
|------|---------|
| `micronaut.server.port` | HTTP server port (default **8082**). |
| `kafka.bootstrap.servers` | Kafka cluster (default **localhost:9092**). |
| `kafka.consumers.default` / `kafka.streams.default` | Consumer and Kafka Streams settings (application id, EOS, state directory, etc.). |
| `kafka.executors.consumer.default.allow.auto.create.topics` | **false** — topics must exist before the app starts. |
| `app.platform.data.folderPath` | Local warehouse / data folder used by the processing pipeline (default points at a `file://` path under the developer machine; **change this** for your environment). |

Environment-specific values can be overridden with Micronaut’s usual mechanisms (environment variables, `MICRONAUT_ENVIRONMENTS`, external `application.yml`, etc.).

### events-producer

Primary file: `events-producer/src/main/resources/application.properties`.

| Property | Purpose |
|----------|---------|
| `micronaut.server.port` | Default **8081**. |
| `kafka.bootstrap.servers` | Default **localhost:9092**. |

## Kafka topics used by events-processor

The stream and consumers expect these topics (create them explicitly because auto-create is disabled):

| Topic | Usage |
|-------|--------|
| `dp-raw-events` | Source topic for the Kafka Streams pipeline. |
| `dp-raw-grouped-events` | Output of the grouping stage; consumed by `IngestRawDataConsumer`. |
| `dp-commit-raw-data-events` | Consumed by `CommitRawDataConsumer` for commit / persistence. |

## Kafka CLI examples

Run these from your Kafka installation directory (adjust if your scripts live elsewhere). Replace `localhost:9092` if your broker differs.

### Create topics

```bash
./bin/kafka-topics.sh --create --topic dp-raw-events --bootstrap-server localhost:9092
./bin/kafka-topics.sh --create --topic dp-raw-grouped-events --bootstrap-server localhost:9092
./bin/kafka-topics.sh --create --topic dp-commit-raw-data-events --bootstrap-server localhost:9092
```

### Consume (inspect outbound topics)

```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dp-raw-grouped-events
```

```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dp-commit-raw-data-events
```

### Produce keyed messages to `dp-raw-events`

Key/value pairs use `:` as the separator (`parse.key=true`, `key.separator=:`).

```bash
./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic dp-raw-events --property parse.key=true --property key.separator=:
```

After the producer starts, paste lines such as:

```text
1:{"type": "app_events", "eventTime": "2026-05-08 10:00:01", "tenantId": "tenant1", "attributes": {"opType": "LOGIN", "userId": "user@user", "result": "SUCCESS"}}
2:{"type": "app_events", "eventTime": "2026-05-08 10:00:02", "tenantId": "tenant1", "attributes": {"opType": "PAYMENT_START", "userId": "user@user", "result": "SUCCESS"}}
3:{"type": "app_events", "eventTime": "2026-05-08 10:00:03", "tenantId": "tenant1", "attributes": {"opType": "LOGOUT", "userId": "user@user", "result": "SUCCESS"}}
```

Then stop the producer with **Ctrl+C** when finished.


### Get session token

```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_SESSION_TOKEN=
```

```aws sts get-session-token --duration-seconds 3600
```

``terraform init -backend-config 'region=ap-southeast-2' -backend-config 'bucket=tfstate-resources-128779316957-ap-southeast-2-an' -lock=true``

### Setup workspace
``terraform workspace list``
``terraform workspace select dev``
``terraform workspace new dev``

### Plan resources
``terraform plan -var-file tfvars/dev.tfvars -out deployment.plan``