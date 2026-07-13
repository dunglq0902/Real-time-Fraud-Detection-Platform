# Real-Time Fraud Detection Platform

A production-grade, real-time fraud detection system built with **Apache Kafka**, **Apache Flink (Java)**, **ClickHouse**, **MinIO**, and **Grafana**.

## Project Objectives

To build a robust streaming data platform for real-time bank transaction fraud detection using Apache Flink as the core streaming processing engine, and achieve near exactly-once semantics.

## Architecture Overview
Data Simulator -> Kafka -> Flink -> ClickHouse -> Grafana
- **Ingestion**: Apache Kafka for high throughput event ingestion (`transactions` and `fraud-rules` topics). Final actionable alerts are also published to the `fraud-alerts` topic.
- **Alert fan-out (optional downstream consumers)**:
  - `fraud-alerts` topic -> Notification Service -> SMS/email/app push
  - `fraud-alerts` topic -> Case Management -> investigation case creation
  - `fraud-alerts` topic -> Core Banking -> transaction/card hold
  - `fraud-alerts` topic -> Audit/Compliance Service
- **Processing**: Apache Flink (Java) for real-time stream processing.
- **ML Inference**: ONNX Runtime embedded directly in the Flink JVM for in-memory inference.
- **Storage**: ClickHouse for processed transactions, final alerts, and materialized analytics tables. MinIO stores trained ML models and Flink checkpoint/savepoint data.
- **BI**: Grafana for real-time dashboards (throughput, latency, fraud stats).

```text
┌──────────────┐    ┌──────────┐    ┌─────────────────────────────────────────┐
│   Data       │    │  Kafka   │    │         Apache Flink (Java)             │
│  Simulator   │───▶│ (KRaft)  │───▶│  ┌──────┐  ┌────────┐  ┌───────────┐  │
│  (Python)    │    │          │    │  │ CEP  │  │ Rules  │  │ Feature   │  │
└──────────────┘    │ Topics:  │    │  │(Java │  │(Broad- │  │Aggregator │  │
                    │ - txns   │    │  │ CEP  │  │ cast   │  │(Keyed     │  │
                    │ - rules  │    │  │ API) │  │ State) │  │ State)    │  │
                    │ - alerts │    │  └──┬───┘  └───┬────┘  └─────┬─────┘  │
                    └──────────┘    │     └──────┬───┘─────────────┘         │
                                    │     ┌──────▼──────────┐                │
                                    │     │ ONNX Runtime    │ ← In-memory   │
                                    │     │ (ML Scoring)    │   inference    │
                                    │     └──────┬──────────┘                │
                                    │     ┌──────▼──────────┐                │
                                    │     │ Decision Engine │                │
                                    │     └──┬──────────┬───┘                │
                                    │        │          │                    │
                                    └────────┼──────────┼────────────────────┘
                                             │          │
                               ┌─────────────▼──┐  ┌───▼──────────┐
                               │  ClickHouse    │  │ Kafka        │
                               │  (Analytics)   │  │ fraud-alerts │
                               └───────┬────────┘  └──────────────┘
                                       │
                               ┌───────▼────────┐
                               │    Grafana     │
                               │  (Dashboard)   │
                               └────────────────┘
```

## Fraud Detection Engines & Decision Logic

The core Flink job is composed of three detection layers that converge into a **Decision Engine**:

1. **CEP Engine (Flink CEP API)**: Detects behavioral fraud patterns using `Pattern`, `PatternStream`, `within` windowing with event-time and watermarks (`bounded out-of-orderness = 5s`).
   - **Account Takeover (ATO)**: 3+ failed logins → success → transfer within 10 min (HIGH/CRITICAL, based on failed login count)
   - **Micro Transactions (Smurfing/Carding)**: 5+ transactions under $10 within 2 min (MEDIUM/HIGH/CRITICAL, based on count and merchant diversity)
   - **Velocity Attack**: 3+ rapid medium-value transfer/payment events within 2 sec (HIGH/CRITICAL)
   - **Impossible Travel**: Financial transactions from >150km apart within 15 minutes (HIGH/CRITICAL, based on distance)
2. **Adaptive Rules Engine (Broadcast State)**: Evaluates dynamic JSON rules sent via Kafka. Uses `KeyedBroadcastProcessFunction` with `MapStateDescriptor`. Supports operators: `GREATER_THAN`, `LESS_THAN`, `EQUALS`, `IN`, `NOT_IN`, plus optional `time_window` constraints.
3. **ML Scoring Engine**: A Random Forest model is trained on synthetic data, exported to ONNX, uploaded to MinIO, and loaded **once** in `RichFlatMapFunction.open()`. The ONNX feature vector is `[amount, hour_of_day, is_foreign, tx_frequency_1h, distance_km]`.

### Decision Engine Hierarchy

The current `DecisionMaker` resolves conflicts with the following order:

- **Critical rule match** → `BLOCK`
- **Critical CEP pattern** → `BLOCK`
- **Non-critical CEP pattern** → `ALERT`
- **ML score > 0.85** → `BLOCK`
- **ML score > 0.5** → `ALERT`
- **Non-critical rule match** → `ALERT`
- **No anomaly detected** → `APPROVE`

## System Performance & Evaluation Criteria

- **System Performance**:
  - Current Docker Compose simulator workload: 500 events/s.
  - Target Throughput: > 1000 events/s (Optimal: > 5000 events/s).
  - Target End-to-End Latency: < 500ms (Acceptable: < 2s). Measured from Kafka `produced_at` to ClickHouse `decided_at`.
- **ML Model Quality**:
  - Precision > 80%, Recall > 75%, F1-Score > 0.78, AUC-ROC > 0.85, AUC-PR > 0.70.
- **Business Metrics**:
  - Fraud Detection Rate (FDR): > 75%.
  - False Positive Rate (FPR): < 2%.
  - Rule Update Time: < 1 second.

## Quick Start

### Prerequisites
- Docker Desktop (with Docker Compose v2)
- At least 8GB RAM allocated to Docker

### Run the Platform

```bash
# Build and start all services
docker compose up --build -d

# Watch the Flink job submission
docker compose logs -f flink-job-submitter

# Watch all logs
docker compose logs -f

# Check service status
docker compose ps
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **Grafana Dashboard** | http://localhost:3000 | admin / admin |
| **Flink Web UI** | http://localhost:8081 | — |
| **MinIO Console** | http://localhost:9001 | admin / password123 |
| **ClickHouse HTTP** | http://localhost:8123 | default / clickhouse123 |

### Verify Data Flow

```bash
# Check transactions are being processed
docker exec clickhouse clickhouse-client \
  --query "SELECT count() FROM fraud_detection.transactions"

# Check fraud detection by source
docker exec clickhouse clickhouse-client \
  --query "SELECT decision_source, count() FROM fraud_detection.transactions GROUP BY decision_source"

# Check end-to-end latency
docker exec clickhouse clickhouse-client \
  --query "SELECT avg(dateDiff('millisecond', produced_at, decided_at)) as avg_latency FROM fraud_detection.transactions WHERE produced_at > '1970-01-02'"
```

### Stop the Platform

```bash
docker compose down -v   # -v removes volumes (fresh start)
```

## Project Structure

```
├── clickhouse/
│   └── init-db.sql                # Database schema + materialized views
├── data-simulator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── simulator.py               # Transaction event generator (Python)
├── flink-job/
│   ├── Dockerfile                  # Multi-stage: Maven build → Flink image
│   ├── pom.xml                     # Maven project (Flink 1.18.1, CEP, ONNX)
│   └── src/main/java/com/frauddetection/
│       ├── FraudDetectionJob.java          # Main pipeline orchestrator
│       ├── model/
│       │   ├── Transaction.java            # Core transaction POJO
│       │   ├── FraudAlert.java             # Alert record
│       │   ├── FraudRule.java              # Dynamic rule from Kafka
│       │   ├── RuleViolation.java          # Rule match result
│       │   ├── DecisionResult.java         # Final output for ClickHouse
│       │   └── AggregateFeatures.java      # ML feature vector
│       ├── cep/
│       │   ├── FraudPatternDetector.java   # 4 CEP patterns (Flink CEP API)
│       │   └── CepAlertMerger.java         # Merge CEP alerts → main stream
│       ├── rules/
│       │   └── RulesEngine.java            # Broadcast State Pattern
│       ├── features/
│       │   └── FeatureAggregator.java      # MapState + TTL (24h)
│       ├── ml/
│       │   └── OnnxModelScorer.java        # ONNX in-memory inference
│       ├── decision/
│       │   └── DecisionMaker.java          # Final decision priority logic
│       ├── sink/
│       │   ├── ClickHouseSink.java         # CheckpointedFunction sink
│       │   └── AlertClickHouseSink.java    # Alerts sink
│       ├── serialization/
│       │   ├── TransactionDeserializer.java
│       │   ├── FraudRuleDeserializer.java
│       │   ├── DecisionResultSerializer.java
│       │   └── FraudAlertSerializer.java
│       └── utils/
│           └── HaversineUtils.java         # Geo distance calculation
├── grafana/
│   ├── Dockerfile
│   └── provisioning/
│       ├── dashboards/
│       │   ├── dashboard.yml
│       │   └── fraud-dashboard.json
│       └── datasources/
│           └── clickhouse.yml
├── ml-model/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── train_model.py              # Random Forest → ONNX export
├── docker-compose.yml               # Full platform orchestration
├── .env                             # Environment configuration
└── README.md
```

## Configuration

Key environment variables (in `.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ROOT_USER` | admin | MinIO access key |
| `MINIO_ROOT_PASSWORD` | password123 | MinIO secret key |
| `CLICKHOUSE_USER` | default | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | clickhouse123 | ClickHouse password |
| `CEP_TRAVEL_DISTANCE_KM` | 150 | Impossible Travel distance threshold |
| `CEP_TRAVEL_WINDOW_MINUTES` | 15 | Impossible Travel time window |

Data simulator settings (in `docker-compose.yml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `EVENTS_PER_SECOND` | 500 | Transaction generation rate used by the current Docker Compose setup |
| `FRAUD_RATIO` | 0.01 | Percentage of fraudulent event sequences used by the current Docker Compose setup |

## Fault Tolerance & Exactly-Once Semantics

The system aims for near exactly-once processing:
- **Flink Checkpointing**: Configured at 60s intervals using exactly-once mode with the HashMap state backend. Checkpoints and savepoints are stored through Flink's S3 filesystem plugin in MinIO (`s3://flink-checkpoints` and `s3://flink-savepoints`).
- **Kafka Consumer**: Commits offsets based on Flink's checkpoints.
- **ClickHouse Sink**: Uses `CheckpointedFunction` to persist buffered and in-flight records in Flink state; combined with `ReplacingMergeTree` engine for deduplication.
- **Savepoints**: Supported for job upgrades via the configured MinIO savepoint path.

## Technical Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Streaming Engine | Apache Flink (Java) | 1.18.1 |
| CEP | Flink CEP API | 1.18.1 |
| Message Broker | Apache Kafka (KRaft) | 3.7.0 |
| ML Inference | ONNX Runtime (Java) | 1.17.3 |
| Model Storage | MinIO | Latest |
| Analytics DB | ClickHouse | 24.3 |
| Dashboard | Grafana | 11.0.0 |
| Build Tool | Maven | 3.9 |
| Java Version | OpenJDK | 11 |
