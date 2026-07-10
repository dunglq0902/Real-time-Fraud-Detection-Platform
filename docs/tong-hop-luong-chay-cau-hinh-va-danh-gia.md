# Tong hop luong chay, cau hinh va danh gia project

Tai lieu nay gom logic van hanh end-to-end cua project Real-Time Fraud Detection Platform, dua tren code va cau hinh hien tai trong repo.

Muc tieu: doc mot file nay la nam duoc he thong chay nhu the nao, Flink xu ly gi, Kafka/ClickHouse/Grafana duoc cau hinh ra sao, project dang tot o dau, con yeu o dau va nen cai tien nhu the nao.

## 1. Tom tat kien truc

Luong chinh:

```text
Data Simulator
  -> Kafka topics: transactions, fraud-rules
  -> Apache Flink job
       1. Deserialize transaction
       2. Rules Engine bang Broadcast State
       3. CEP Engine bang Flink CEP
       4. CepAlertMerger gan CEP alert ve transaction
       5. FeatureAggregator tinh feature theo account
       6. OnnxModelScorer cham diem ML
       7. DecisionMaker ra APPROVE / ALERT / BLOCK
  -> ClickHouse: transactions, fraud_alerts
  -> Kafka topic: fraud-alerts
  -> Grafana dashboard doc tu ClickHouse
```

MinIO nam ngoai duong du lieu giao dich, dung de luu model ONNX va Flink checkpoint/savepoint.

## 2. Cac thanh phan Docker Compose

File chinh: `docker-compose.yml`.

| Service | Vai tro | Cau hinh dang chu y |
|---|---|---|
| `kafka` | Message broker, chay KRaft khong ZooKeeper | image `apache/kafka:3.7.0`, port `9092`, `9093`, 1 broker |
| `kafka-init` | Tao Kafka topics | `transactions` 8 partitions, `fraud-rules` 1 partition, `fraud-alerts` 3 partitions |
| `minio` | Object storage S3-compatible | port `9000`, console `9001`, user/pass tu `.env` |
| `minio-init` | Tao bucket | `ml-models`, `flink-checkpoints`, `flink-savepoints` |
| `clickhouse` | OLAP storage cho dashboard | image `clickhouse/clickhouse-server:24.3`, HTTP `8123`, native mapped `9004:9000` |
| `flink-jobmanager` | Dieu phoi Flink cluster | Flink 1.18.1 Java 11, memory 1536m |
| `flink-taskmanager` | Chay Flink operators | memory 3584m, 8 task slots |
| `ml-trainer` | Train Random Forest, export ONNX, upload MinIO | chay mot lan sau `minio-init` |
| `flink-job-submitter` | Submit Java Flink job | doi 30s roi `flink run` JAR |
| `data-simulator` | Kafka producer sinh transaction/rule | `EVENTS_PER_SECOND=500`, `FRAUD_RATIO=0.01` |
| `grafana` | Dashboard | port `3000`, plugin `grafana-clickhouse-datasource` |

Bien moi truong trong `.env`:

```text
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse123
CEP_TRAVEL_DISTANCE_KM=150
CEP_TRAVEL_WINDOW_MINUTES=15
```

Luu y: README co noi RocksDB, nhung `docker-compose.yml` hien dang cau hinh `state.backend: hashmap`. Dependency RocksDB co trong Maven, nhung runtime hien tai la HashMap State Backend.

## 3. Kafka

Kafka chay single broker o local demo.

Topics:

| Topic | Partitions | Replication factor | Muc dich |
|---|---:|---:|---|
| `transactions` | 8 | 1 | Chua transaction events tu simulator |
| `fraud-rules` | 1 | 1 | Chua JSON rule update |
| `fraud-alerts` | 3 | 1 | Output alert cuoi cung de mo rong notification/case management |

Simulator produce transaction voi key la `account_id`, giup cac event cung account co xu huong vao cung Kafka partition. Flink van tiep tuc `keyBy(accountId)` de dam bao state theo account.

Offset trong Flink:

- `transactions`: `OffsetsInitializer.latest()`.
- `fraud-rules`: `OffsetsInitializer.earliest()`.

Danh gia quan trong: `data-simulator` khong phu thuoc `flink-job-submitter`, nen co the bat dau gui transaction truoc khi Flink job start. Vi transaction source dung `latest`, mot so transaction sinh truoc luc source start co the bi bo qua. Rule source dung `earliest` nen rule ban dau van doc lai duoc.

## 4. Data Simulator

File chinh: `data-simulator/simulator.py`.

Simulator tao du lieu gia lap co hanh vi:

- 500 account: `ACC-000001` den `ACC-000500`.
- 200 merchant: `MERCH-0001` den `MERCH-0200`.
- Moi account co mot card tuong ung.
- Channel: `ATM`, `ONLINE`, `POS`, `MOBILE`.
- Event type binh thuong: `LOGIN`, `TRANSFER`, `PAYMENT`, `WITHDRAWAL`, `CHANGE_PASSWORD`.
- Event type trong fraud sequence: `LOGIN_FAILED`, `LOGIN_SUCCESS`.
- Location gom Viet Nam va quoc te: Ho Chi Minh, Hanoi, Da Nang, Singapore, Tokyo, New York, London, Sydney, Dubai, Seoul.

Schema transaction duoc tao:

```json
{
  "transaction_id": "uuid",
  "account_id": "ACC-000001",
  "card_id": "CARD-00000001",
  "timestamp": 1710000000000,
  "produced_at": 1710000000100,
  "amount": 120.5,
  "event_type": "PAYMENT",
  "channel": "ONLINE",
  "location": "{\"latitude\":10.8231,\"longitude\":106.6297}",
  "status": "PENDING",
  "merchant_id": "MERCH-0001",
  "is_fraud": 0
}
```

Y nghia cac truong:

| Truong | Y nghia |
|---|---|
| `transaction_id` | ID duy nhat cua event |
| `account_id` | Khoa de gom state hanh vi theo tai khoan |
| `timestamp` | Event time dung cho watermark/CEP |
| `produced_at` | Thoi diem producer gui event, dung tinh latency |
| `amount` | So tien giao dich |
| `event_type` | Loai hanh vi/giao dich |
| `location` | JSON string chua latitude/longitude |
| `is_fraud` | Nhan gia lap tu simulator |

Cac kieu fraud simulator sinh:

| Kieu | Ham | Mo ta | Engine bat chinh |
|---|---|---|---|
| Account takeover | `generate_brute_force_sequence` | 3-5 `LOGIN_FAILED` -> `LOGIN_SUCCESS` -> `TRANSFER` 5,000-50,000 | CEP ATO |
| Rapid transactions | `generate_rapid_transactions` | 3-6 giao dich nhanh, amount 100-2,000 | CEP velocity |
| Impossible travel | `generate_impossible_travel` | 2 giao dich cach nhau >150km trong thoi gian ngan | CEP impossible travel |
| High amount | `generate_high_amount_transaction` | 10,000-100,000 | Rule high amount, ML |
| Micro transactions | `generate_micro_transactions` | 5-8 giao dich duoi 10 | CEP micro transactions |

Initial rules duoc push vao topic `fraud-rules` khi simulator start:

| Rule | Dieu kien | Severity |
|---|---|---|
| `RULE_001` High Amount Threshold | `amount > 10000` | `HIGH` |
| `RULE_002` Blacklisted Merchant | `merchant_id IN [MERCH-0199, MERCH-0198, MERCH-0197]` | `CRITICAL` |
| `RULE_003` Nighttime Large Transfer | `amount > 5000` va gio 0-5 | `MEDIUM` |

## 5. ML Trainer va model ONNX

File chinh: `ml-model/train_model.py`.

Luong train:

```text
generate_synthetic_data(50000)
  -> train_test_split stratify
  -> RandomForestClassifier
  -> evaluate classification report, AUC-ROC, AUC-PR
  -> export ONNX bang skl2onnx
  -> verify ONNX bang onnxruntime
  -> upload MinIO: s3://ml-models/fraud_detector.onnx
```

Feature model:

```text
[amount, hour_of_day, is_foreign, tx_frequency_1h, distance_km]
```

Model:

- `RandomForestClassifier`
- `n_estimators=100`
- `max_depth=10`
- `class_weight="balanced"`
- output ONNX: `fraud_detector.onnx`

Danh gia: cach nay tot cho demo vi model duoc tao tu dong trong Docker Compose. Tuy nhien day la synthetic model, chua phai model duoc validate tren du lieu fraud that.

## 6. Flink job tong quan

File chinh: `flink-job/src/main/java/com/frauddetection/FraudDetectionJob.java`.

Pipeline:

```text
KafkaTransactionSource
  -> FilterInvalidTransactions
  -> RulesEngine
  -> FraudPatternDetector.detectAll
  -> CepAlertMerger
  -> FeatureAggregator
  -> OnnxModelScorer
  -> DecisionMaker
  -> ClickHouseTransactionSink
  -> AlertExtractor
  -> ClickHouseAlertSink
  -> KafkaAlertSink
```

Cau hinh Flink trong code:

```text
parallelism = 8
checkpoint interval = 60000 ms
checkpoint mode = EXACTLY_ONCE
min pause = 30000 ms
checkpoint timeout = 120000 ms
max concurrent checkpoints = 1
unaligned checkpoints = enabled
externalized checkpoint cleanup = RETAIN_ON_CANCELLATION
```

Cau hinh Flink trong Docker Compose:

```text
state.backend = hashmap
state.checkpoints.dir = s3://flink-checkpoints/checkpoints
state.savepoints.dir = s3://flink-savepoints/savepoints
s3.endpoint = http://minio:9000
taskmanager.numberOfTaskSlots = 8
parallelism.default = 8
```

Watermark transaction:

```text
event time = tx.timestamp
bounded out-of-orderness = 5 seconds
idleness = 30 seconds
```

Y nghia: CEP xu ly theo thoi gian su kien, chap nhan event den lech toi da 5 giay va tranh watermark bi ket khi Kafka partition idle.

## 7. Transaction deserialization

File: `TransactionDeserializer.java`, model: `Transaction.java`.

Deserializer:

1. Doc JSON bytes tu Kafka.
2. Jackson map sang `Transaction`.
3. Goi `tx.parseLocation()` de tach `latitude`, `longitude` tu field `location`.
4. Neu loi parse thi return `null`.
5. Pipeline filter bo record null hoac thieu `transaction_id`.

`parseLocation()` dung string parsing thu cong thay vi ObjectMapper cho nested JSON nho. Diem tot la giam overhead moi event. Diem can canh giac la parser thu cong de vo neu format location thay doi.

## 8. Rules Engine

File: `RulesEngine.java`.

Muc dich: xu ly rule update dong tu Kafka bang Broadcast State.

Luong rule:

```text
Kafka fraud-rules
  -> FraudRuleDeserializer
  -> BroadcastStream<String>
  -> MapStateDescriptor("fraud-rules")
  -> moi subtask RulesEngine co cung bo rule
```

Khi co rule update:

- Parse JSON thanh `FraudRule`.
- Lay `rule_id`.
- So version voi rule hien co.
- Chi cap nhat neu version moi hon.
- Luu raw JSON vao Broadcast State.
- Luu object da parse vao local `ruleCache` de tranh parse JSON tren tung transaction.

Khi co transaction:

- Rebuild cache tu Broadcast State neu cache rong sau restart.
- Duyet cac rule active.
- Lay field tu transaction.
- So sanh theo operator.
- Neu rule co `time_window`, check theo timezone `Asia/Ho_Chi_Minh`.
- Tao `RuleViolation` va `FraudAlert` object neu match.
- Gan vao transaction: `ruleAlerts`, `ruleAlertObjects`, `ruleFlagged=true`.

Operator ho tro:

```text
GREATER_THAN, LESS_THAN, EQUALS, IN, NOT_IN
```

Field ho tro:

```text
amount, event_type, channel, merchant_id, status, card_id, account_id
```

Score rule hien tai:

```text
CRITICAL -> 0.8
khac CRITICAL -> 0.6
```

## 9. CEP Engine

File: `FraudPatternDetector.java`.

Input la stream da qua RulesEngine va `keyBy(Transaction::getAccountId)`. CEP detect 4 pattern, sau do union thanh mot stream `DataStream<FraudAlert>`.

Nguong co the cau hinh qua env:

| Env | Default | Y nghia |
|---|---:|---|
| `CEP_ATO_MIN_FAILED_LOGINS` | 3 | So failed login toi thieu |
| `CEP_ATO_WINDOW_MINUTES` | 10 | Cua so ATO |
| `CEP_MICRO_MIN_TRANSACTIONS` | 5 | So micro tx toi thieu |
| `CEP_MICRO_MAX_AMOUNT` | 10.0 | Amount toi da cua micro tx |
| `CEP_MICRO_WINDOW_MINUTES` | 2 | Cua so micro tx |
| `CEP_VELOCITY_MIN_TRANSACTIONS` | 3 | So giao dich nhanh toi thieu |
| `CEP_VELOCITY_MIN_AMOUNT` | 100.0 | Amount toi thieu cua velocity tx |
| `CEP_VELOCITY_MAX_AMOUNT` | 2000.0 | Amount toi da cua velocity tx |
| `CEP_VELOCITY_WINDOW_SECONDS` | 2 | Cua so velocity tx |
| `CEP_TRAVEL_DISTANCE_KM` | 150.0 | Khoang cach impossible travel |
| `CEP_TRAVEL_WINDOW_MINUTES` | 15 | Cua so impossible travel |

Trong `docker-compose.yml` hien chi set `CEP_TRAVEL_DISTANCE_KM` va `CEP_TRAVEL_WINDOW_MINUTES`; cac nguong con lai dung default.

Pattern 1 - Account Takeover:

```text
>= 3 LOGIN_FAILED
  -> LOGIN_SUCCESS
  -> TRANSFER
within 10 minutes
```

- Dung `AfterMatchSkipStrategy.skipPastLastEvent()` de giam duplicate/overlap.
- Score = `min(0.99, 0.80 + failedCount * 0.03)`.
- Severity = `CRITICAL` neu failedCount >= 7, nguoc lai `HIGH`.
- Luu y: simulator sinh 3-5 failed login, nen ATO thuong la `HIGH` va DecisionMaker se `ALERT`, khong `BLOCK`, tru khi co rule/ML khac day len.

Pattern 2 - Micro Transactions:

```text
>= 5 financial transactions
amount < 10
within 2 minutes
```

Financial event types: `TRANSFER`, `PAYMENT`, `WITHDRAWAL`.

- Score = `min(0.98, 0.75 + count * 0.03)`.
- Severity:
  - `MEDIUM` neu chi 1 merchant.
  - `HIGH` neu 2+ merchants va count < 10.
  - `CRITICAL` neu 3+ merchants hoac count >= 10.
- Dung rolling window va emit alert cho tung transaction trong matched window de metric event-level khong chi dem event cuoi chuoi.

Pattern 3 - Velocity Attack:

```text
>= 3 TRANSFER/PAYMENT
amount 100..2000
within 2 seconds
```

- Dung rolling window `skipToNext()` de bat cac chuoi rapid 3-6 giao dich.
- Emit alert cho tung transaction trong matched window.
- Score = `min(0.95, 0.70 + count * 0.05)`.
- Severity = `CRITICAL` neu span <= 1 giay hoac count >= 5, nguoc lai `HIGH`.

Pattern 4 - Impossible Travel:

```text
2 financial transactions
khoang cach >= 150km
within 15 minutes
```

- Dung Haversine de tinh khoang cach.
- Reject toa do invalid hoac `(0,0)`.
- Score = `min(0.99, 0.80 + distance / 10000.0)`.
- Severity = `CRITICAL` neu distance >= 2000km, nguoc lai `HIGH`.

## 10. CepAlertMerger

File: `CepAlertMerger.java`.

Van de: nhanh transaction chinh di nhanh hon nhanh CEP. Neu khong dong bo, transaction co the vao DecisionMaker va duoc `APPROVE` truoc khi CEP alert toi.

Giai phap:

- Buffer transaction theo `transaction_id`.
- Cho `BUFFER_DELAY_MS = 10ms`.
- Neu CEP alert den khi transaction con buffer, gan alert vao transaction.
- Neu alert den truoc transaction, luu vao `orphanAlerts` va `orphanIndex`.
- Neu alert den sau khi transaction da emit, emit lai mot transaction copy voi `lateAlert=true`.

State TTL:

| State | TTL |
|---|---:|
| buffered transactions | 60s |
| timer map | 60s |
| orphan alerts | 30s |
| emitted transactions | 60s |

Diem tot: co xu ly race condition giua CEP branch va main branch. Diem can chu y: late alert co the tao ban ghi quyet dinh bo sung cho cung transaction; can dashboard/query dedup dung cach.

## 11. FeatureAggregator

File: `FeatureAggregator.java`.

Muc dich: tinh feature hanh vi theo account bang Flink Keyed State.

Feature gan vao transaction:

| Feature | Y nghia |
|---|---|
| `txCount1h` | So giao dich 1 gio gan nhat |
| `txCount24h` | So giao dich 24 gio gan nhat |
| `avgAmount24h` | Amount trung binh 24 gio |
| `amountDeviation` | Do lech amount hien tai so voi trung binh/std dev |
| `timeSinceLastMs` | Khoang thoi gian tu transaction truoc |
| `distanceKm` | Khoang cach voi location transaction truoc |

Thiet ke state:

- Dung `ValueState` cho counter/sum.
- Dung `MapState` lam timer index de giam counter khi event het window.
- TTL 25h cho state 24h, 2h cho state 1h.
- Dung processing-time timer thay vi event-time timer de tranh watermark stall lam state khong cleanup.
- Tinh counter O(1) moi transaction, khong scan toan bo lich su.

Short-circuit:

- Neu transaction da co alert `CRITICAL` tu Rules/CEP thi bo qua feature computation.
- Muc tieu: giam CPU va tranh fraud critical lam nhieu behavioral profile.

## 12. ONNX ML Scoring

File: `OnnxModelScorer.java`.

Luong chay:

```text
open()
  -> download s3://ml-models/fraud_detector.onnx tu MinIO
  -> tao ONNX Runtime session
  -> lay input name
  -> pre-allocate float[1][5]

flatMap(tx)
  -> neu lateAlert: mlScore=0, mlAvailable=false
  -> neu critical alert: short-circuit
  -> neu model load fail: mlScore=0, mlAvailable=false
  -> extract feature vector
  -> run ONNX inference
  -> set mlScore va mlAvailable=true
```

Feature vector dung cho ONNX:

```text
[amount, hour_of_day, is_foreign, tx_frequency_1h, distance_km]
```

`is_foreign` duoc tinh bang bounding box Viet Nam:

```text
lat 8.0..23.5 va lon 102.0..110.0 -> domestic
nguoc lai -> foreign
```

Diem tot:

- Model chay in-process trong JVM, khong ton network round trip.
- Model load mot lan trong `open()`.
- Pre-allocate input buffer de giam GC.
- Neu model loi, pipeline van tiep tuc bang Rules/CEP.

Diem han che:

- Model update can restart job hoac them co che reload.
- Chua luu `model_version` vao output.
- FeatureAggregator tinh nhieu feature hon model dang dung, vi model chi dung `txCount1h` va `distanceKm` tu aggregate features.

## 13. DecisionMaker

File: `DecisionMaker.java`.

Input: transaction da co rule alerts, CEP alerts va ML score.

Output: `DecisionResult` ghi vao ClickHouse.

Logic hien tai:

```text
if hasCriticalRule:
  decision = BLOCK, source = RULES_ENGINE
else if hasCriticalCep:
  decision = BLOCK, source = CEP_ENGINE
else if hasSuspiciousCep:
  decision = ALERT, source = CEP_ENGINE
else if mlScore > 0.85:
  decision = BLOCK, source = ML_SCORING
else if mlScore > 0.5:
  decision = ALERT, source = ML_SCORING
else if ruleFlagged:
  decision = ALERT, source = RULES_ENGINE
else:
  decision = APPROVE, source = NONE
```

Nguong ML:

```text
ML_BLOCK_THRESHOLD = 0.85
ML_ALERT_THRESHOLD = 0.5
```

`combinedScore` = max cua `mlScore`, max CEP score, max rule score.

Diem can chu y: comment/tai lieu noi hierarchy la `Rules > CEP > ML`, nhung implementation hien tai chi uu tien `CRITICAL` rule truoc CEP/ML. Rule non-critical dang bi dat sau ML. Neu muon "Rules > CEP > ML" nghiem ngat, can dua `ruleFlagged` non-critical len truoc ML.

## 14. AlertExtractor va Kafka alerts

Sau `DecisionMaker`, job chi tao final alert cho decision `ALERT` hoac `BLOCK`.

`toFinalFraudAlert()` tao:

- `alert_id = FINAL_<transaction_id>` de on dinh hon random UUID.
- `alert_type = FINAL_ALERT` hoac `FINAL_BLOCK`.
- `alert_source` dua tren `decision_source`.
- `severity = CRITICAL` neu `BLOCK`, nguoc lai `HIGH`.
- `details` gom final decision, source, combined score, ml score, rule triggered.

Output alert di 2 noi:

- ClickHouse table `fraud_detection.fraud_alerts`.
- Kafka topic `fraud-alerts`.

Danh gia: chi luu final alert giup dashboard gon, nhung mat audit chi tiet cua raw Rules/CEP alerts. Neu can compliance, nen them bang raw alerts.

## 15. ClickHouse schema

File: `clickhouse/init-db.sql`.

Database:

```sql
CREATE DATABASE IF NOT EXISTS fraud_detection;
```

Bang `fraud_detection.transactions`:

- Engine: `ReplacingMergeTree(decided_at)`.
- Partition: `toYYYYMMDD(event_time)`.
- Order key: `(transaction_id)`.
- TTL: `event_time + 90 day`.

Cot chinh:

```text
transaction_id, account_id, card_id, event_time, amount,
event_type, channel, latitude, longitude, merchant_id, status,
ml_score, rule_triggered, is_fraud, ground_truth_is_fraud,
decision, decision_source, combined_score, produced_at,
decided_at, processing_time
```

Trong bang output:

- `is_fraud`: fraud flag do `DecisionMaker` phat hien, tuong ung `ALERT`/`BLOCK`.
- `ground_truth_is_fraud`: nhan goc tu simulator, dung de tinh metric evaluation.

Bang `fraud_detection.fraud_alerts`:

- Engine: `ReplacingMergeTree(alert_time)`.
- Partition: `toYYYYMMDD(event_time)`.
- Order key: `(alert_id)`.
- TTL: `alert_time + 180 day`.

Cot chinh:

```text
alert_id, transaction_id, account_id, alert_type, alert_source,
severity, score, details, matched_events, event_time, alert_time
```

Materialized views:

| View/table | Muc dich |
|---|---|
| `tx_throughput_mv` -> `tx_throughput` | Count, total amount, fraud count theo phut |
| `alerts_by_type_mv` -> `alerts_by_type` | Count alert theo ngay/type/source |
| `fraud_by_geo_mv` -> `fraud_by_geo` | Fraud theo bucket latitude/longitude |
| `latency_stats_mv` -> `latency_stats` | Avg/max latency theo phut |

Diem can chu y: `ReplacingMergeTree` dedup bat dong bo sau merge. Query count tren bang goc co the dem duplicate tam thoi neu khong dung `FINAL` hoac `argMax`.

## 16. ClickHouse sinks

Files:

- `ClickHouseSink.java`
- `AlertClickHouseSink.java`

Transaction sink:

```text
table = fraud_detection.transactions
maxBatchSize = 500
flushIntervalMs = 2000
HTTP timeout = 3000 ms
```

Alert sink:

```text
table = fraud_detection.fraud_alerts
maxBatchSize = 100
flushIntervalMs = 3000
HTTP timeout = 3000 ms
```

Thiet ke:

- Custom `RichSinkFunction`.
- Implement `CheckpointedFunction`.
- Buffer record trong memory.
- Flush HTTP async bang single background thread.
- `snapshotState()` luu buffer va inflight records vao Flink operator state.
- Neu HTTP flush loi, re-add snapshot vao buffer neu chua vuot cap.
- Neu loi dai han va buffer vuot cap, co the drop record de tranh memory tang vo han.

Danh gia dam bao:

- Flink source/state co checkpoint exactly-once.
- ClickHouse sink la near exactly-once, khong phai two-phase commit.
- Idempotency dua vao stable key va `ReplacingMergeTree`.
- Van co rui ro duplicate tam thoi, duplicate lau hon neu merge chua chay, hoac drop khi ClickHouse loi dai han.

## 17. Grafana

Files:

- `grafana/provisioning/datasources/clickhouse.yml`
- `grafana/provisioning/dashboards/dashboard.yml`
- `grafana/provisioning/dashboards/fraud-dashboard.json`

Datasource:

```text
name = ClickHouse
type = grafana-clickhouse-datasource
server = clickhouse
port = 8123
protocol = http
defaultDatabase = fraud_detection
username = default
password = clickhouse123
```

Dashboard panels chinh:

| Panel | Query/metric |
|---|---|
| Total Transactions | `count()` tu `transactions` |
| Fraud Detected | `count()` voi detected flag `is_fraud = 1` |
| Ground Truth Fraud Rate | `countIf(ground_truth_is_fraud = 1) / count()` |
| BLOCKED Transactions | `decision = 'BLOCK'` |
| Avg ML Score | `avg(ml_score)` voi `ml_score > 0` |
| Total Alerts | `uniqExact(alert_id)` |
| Throughput | record trong 1 phut gan nhat / 60 |
| FDR/FPR | cong thuc TP/FP dua tren `ground_truth_is_fraud` va detected flag `is_fraud` |
| End-to-End Latency | `dateDiff('millisecond', produced_at, decided_at)` |
| Throughput time series | count/fraud theo phut |
| Latency p95/p99 | quantile latency theo phut |
| Alerts by Source/Type | group theo alert source/type |
| Decision Breakdown | group theo `decision` |
| Decision Source Breakdown | group theo `decision_source` |
| Recent Fraud Alerts | bang alert gan nhat |

Can luu y: `is_fraud` trong ClickHouse la output cua `DecisionMaker`; `ground_truth_is_fraud` moi la nhan goc tu simulator. Dashboard da dung hai cot nay de tinh FDR/FPR theo ground truth.

## 18. Fault tolerance va delivery semantics

Diem da co:

- Flink checkpoint 60s, mode exactly-once.
- Externalized checkpoint retain on cancellation.
- Checkpoint/savepoint ghi vao MinIO S3-compatible.
- Kafka source offset gan voi checkpoint.
- State cua Rules/CEP/FeatureAggregator/Sink duoc checkpoint.
- ClickHouse sink luu buffer/inflight vao operator state.
- ClickHouse dung `ReplacingMergeTree` de dedup gan idempotent.

Gioi han:

- Kafka cluster local co 1 broker, replication factor 1.
- ClickHouse sink custom khong co 2PC.
- Kafka alert sink khong set delivery guarantee ro rang trong code.
- Neu ClickHouse loi dai han, sink co logic drop record khi buffer vuot cap.
- Transaction source dung `latest`, co the bo qua backlog/demo events truoc luc job start.

Ket luan: nen goi day la "near exactly-once end-to-end to ClickHouse", khong nen noi exactly-once tuyet doi.

## 19. Danh gia diem manh

1. Kien truc dung bai toan streaming

Kafka tach producer/consumer, Flink xu ly stateful realtime, ClickHouse phu hop dashboard analytics, Grafana quan sat truc quan.

2. Multi-layer detection

He thong khong phu thuoc duy nhat vao ML. Rules bat dieu kien ro rang, CEP bat chuoi hanh vi, ML bat pattern xac suat.

3. Co explainability

Output co `decision`, `decision_source`, `rule_triggered`, `ml_score`, `combined_score`, giup giai thich vi sao giao dich bi alert/block.

4. Rules update dong

Broadcast State cho phep cap nhat rule runtime qua Kafka ma khong restart Flink job.

5. CEP duoc thiet ke can than

Co event time, watermark, skip strategy de giam duplicate, Haversine cho impossible travel va threshold co the cau hinh.

6. State management co y thuc ve hieu nang

FeatureAggregator dung counter O(1), TTL, processing-time cleanup timer. CepAlertMerger co TTL cho buffer/orphan state.

7. ML inference latency thap

ONNX Runtime chay trong JVM, model load mot lan, khong can network call toi model-serving service.

8. Sink da co toi uu latency

ClickHouse HTTP flush chay background thread, khong block truc tiep operator thread tren moi batch.

9. Schema ClickHouse phu hop analytics

Co `ReplacingMergeTree`, partition theo ngay, TTL, materialized views cho throughput/latency/alert/geography.

10. Co dashboard va Docker Compose end-to-end

Project de demo, de trinh bay, co du stack tu generator toi dashboard.

11. Co unit tests cho cac logic nen tang

Da co test cho Haversine, deserialization, rule model va DecisionMaker.

## 20. Danh gia diem chua tot va giai phap

| Van de | Anh huong | Giai phap de xuat |
|---|---|---|
| Ground truth hien van la synthetic label tu simulator | Metric FDR/FPR da tinh dung theo nhan goc, nhung chua phan anh du lieu fraud thuc te | Train/validate bang du lieu that hoac public fraud dataset, them quy trinh labeling/feedback |
| `data-simulator` co the gui transaction truoc khi Flink source start | Vi source dung `latest`, co the miss event dau | Cho simulator depend vao `flink-job-submitter` hoac dung `earliest`/committed offsets cho demo |
| Single Kafka broker, RF=1 | Khong chiu loi broker | Production dung 3+ brokers, RF=3, min ISR, monitoring |
| State backend dang la HashMap | State lon gay ap luc heap/memory | Chuyen sang RocksDB/EmbeddedRocksDB cho state lon va checkpoint incremental |
| README/tai lieu noi RocksDB nhung Compose la HashMap | De bi hoi khi bao ve/van hanh | Dong bo tai lieu voi cau hinh hoac doi config sang RocksDB |
| ClickHouse sink custom near exactly-once | Co rui ro duplicate/drop khi loi dai han | Dung sink chuan/idempotent hon, staging table + merge, retry queue, dead-letter topic |
| Query dashboard tren `transactions` khong dedup bat buoc | Duplicate tam thoi co the lam sai count | Dung `argMax` theo `transaction_id`, query `FINAL` co chon loc, hoac materialized final table |
| Kafka `fraud-alerts` sink chua set delivery guarantee | Output alert Kafka co semantics khong ro | Cau hinh `DeliveryGuarantee.AT_LEAST_ONCE` hoac `EXACTLY_ONCE` kem transactional id prefix |
| Rule priority doc/code chua khop | Noi `Rules > CEP > ML` nhung non-critical rule nam sau ML | Neu muon strict priority, dua non-critical `ruleFlagged` len truoc ML |
| ATO simulator sinh 3-5 failed login, CEP critical can >=7 | ATO thuong chi ALERT, khong BLOCK | Giam critical threshold hoac cho simulator sinh 7+ failed login khi muon demo block |
| Model train synthetic | Ket qua ML de overfit vao quy tac gia lap | Train/validate bang du lieu that hoac public fraud dataset, calibration, drift monitoring |
| Model khong reload dong | Update model can restart job | Them model registry/versioning, broadcast model metadata, reload theo timer/savepoint |
| Chua luu `model_version` | Kho trace quyet dinh theo phien ban model | Them `model_version` vao DecisionResult/ClickHouse/Grafana |
| `location` la JSON string va parse thu cong | Fragile khi schema thay doi | Dung nested object co schema ro, Avro/Protobuf/JSON Schema |
| Chua co Schema Registry | Producer/consumer de lech schema | Them Schema Registry hoac validation layer |
| Secrets de trong `.env`/Grafana provisioning | Khong phu hop production | Dung Docker secrets, Vault, env injection, rotation |
| Grafana chu yeu doc ClickHouse, chua co Flink/Kafka metrics | Kho debug backpressure/checkpoint lag | Them Prometheus + Flink metrics + Kafka exporter + ClickHouse exporter |
| Tests con thieu integration | CEP, sink, end-to-end chua duoc test tu dong | Them Flink MiniCluster tests, Testcontainers Kafka/ClickHouse, compose smoke test |
| Alert table chi luu final alert | Thieu audit raw signal | Them `raw_alerts`/`rule_violations`/`cep_alerts` table de audit |

## 21. Muc do san sang cua project

Neu danh gia theo muc tieu hoc tap/demo/portfolio:

```text
8/10
```

Ly do: kien truc day du, co Kafka/Flink/CEP/Rules/ML/ClickHouse/Grafana, co Docker Compose, co dashboard, logic kha giau va co nhieu toi uu state/sink.

Neu danh gia theo muc tieu production banking:

```text
5/10
```

Ly do: can hardening ve HA, security, schema governance, validation tren du lieu that, delivery guarantee, observability, model governance va testing.

## 22. Uu tien cai tien nen lam truoc

Uu tien 1 - Hoan thien evaluation:

- Da luu `ground_truth_is_fraud` tu simulator vao output.
- Da sua ClickHouse schema va Grafana FDR/FPR theo ground truth.
- Nen them them panel Precision/Recall, TP/FP/TN/FN count va query dedup theo `transaction_id` khi bao cao chinh thuc.

Uu tien 2 - Dong bo luong demo:

- Cho `data-simulator` start sau khi Flink job da submit.
- Hoac doi transaction source sang `earliest` trong demo.
- Sua README/docs de khop `EVENTS_PER_SECOND=500` va `state.backend=hashmap`.

Uu tien 3 - Lam metric evaluation sau hon:

- Them panel Precision/Recall/F1 va confusion matrix.
- Them case-level metric theo `fraud_sequence_id` neu simulator bo sung sequence metadata.
- Theo doi FDR/FPR rieng theo `fraud_type`.

Uu tien 4 - Lam sink chac hon:

- Kafka alert sink set delivery guarantee.
- ClickHouse query/sink idempotent ro hon.
- Them retry/dead-letter khi ClickHouse loi dai han.

Uu tien 5 - Production hardening:

- Kafka 3 brokers, replication factor 3.
- RocksDB state backend.
- Prometheus/Grafana infra metrics.
- Secrets management.
- CI/CD voi savepoint.
- Model versioning va drift monitoring.

## 23. Cau noi ngan gon khi bao ve project

He thong nay la mot pipeline fraud detection realtime end-to-end. Simulator sinh transaction va rule update vao Kafka. Flink doc stream theo event time, chay qua 3 lop phat hien gom Rules Engine bang Broadcast State, CEP Engine bat chuoi hanh vi va ML Scoring bang ONNX Runtime trong JVM. DecisionMaker hop nhat cac tin hieu thanh `APPROVE`, `ALERT` hoac `BLOCK`, sau do ghi ket qua vao ClickHouse va hien thi tren Grafana. Diem manh la kien truc stateful realtime, co explainability, co ground truth metric cho du lieu gia lap va co dashboard; diem can cai tien la delivery guarantee, HA/security, model governance va validation tren du lieu that neu dua len production.
