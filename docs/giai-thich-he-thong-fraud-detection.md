# Giải thích đầy đủ hệ thống Real-Time Fraud Detection Platform

Tài liệu này dùng để nắm kiến trúc, giải thích luồng chạy, hiểu cách tính điểm và chuẩn bị phản biện trước hội đồng. Nội dung bám theo code hiện tại của project.

## 1. Mục tiêu hệ thống

Hệ thống mô phỏng một nền tảng phát hiện gian lận giao dịch ngân hàng theo thời gian thực.

Mục tiêu kỹ thuật:

- Sinh giao dịch liên tục bằng `data-simulator`.
- Đưa giao dịch vào Kafka để làm hàng đợi sự kiện.
- Xử lý stream bằng Apache Flink.
- Kết hợp 3 lớp phát hiện gian lận:
  - Dynamic Rules Engine: luật có thể cập nhật qua Kafka.
  - CEP Engine: phát hiện chuỗi hành vi bất thường theo thời gian.
  - ML Scoring: mô hình Random Forest được export sang ONNX, chạy trực tiếp trong JVM của Flink.
- Ghi kết quả cuối cùng vào ClickHouse.
- Hiển thị dashboard thời gian thực bằng Grafana.
- Tạo alert cuối cùng vào ClickHouse và Kafka topic `fraud-alerts` để có thể mở rộng sang SMS, email, case management hoặc core banking.

Mục tiêu nghiệp vụ:

- Với giao dịch bình thường: quyết định `APPROVE`.
- Với giao dịch nghi ngờ: quyết định `ALERT`.
- Với giao dịch rủi ro cao: quyết định `BLOCK`.
- Giải thích được vì sao hệ thống ra quyết định: do rule, do CEP hay do ML.

## 2. Kiến trúc tổng thể

Luồng chính:

```text
Data Simulator
    |
    v
Kafka topics
    - transactions
    - fraud-rules
    - fraud-alerts
    |
    v
Apache Flink Job
    1. Kafka Source + deserialization
    2. Rules Engine bằng Broadcast State
    3. CEP Pattern Detector
    4. CepAlertMerger
    5. FeatureAggregator bằng Keyed State
    6. ONNX ML Scorer
    7. DecisionMaker
    |
    +--> ClickHouse transactions table
    +--> ClickHouse fraud_alerts table
    +--> Kafka fraud-alerts topic
    |
    v
Grafana dashboard
```

Các thành phần trong Docker Compose:

- `kafka`: message broker chạy ở KRaft mode, không dùng ZooKeeper.
- `kafka-init`: tạo 3 topic Kafka.
- `minio`: S3-compatible object storage, dùng để lưu model ONNX và checkpoint/savepoint Flink.
- `minio-init`: tạo bucket `ml-models`, `flink-checkpoints`, `flink-savepoints`.
- `clickhouse`: database phân tích dạng cột.
- `flink-jobmanager`: điều phối Flink job.
- `flink-taskmanager`: thực thi operator của Flink.
- `ml-trainer`: train model, export ONNX và upload lên MinIO.
- `flink-job-submitter`: submit Java Flink job sau khi Kafka, Flink và model đã sẵn sàng.
- `data-simulator`: sinh giao dịch liên tục.
- `grafana`: đọc ClickHouse để hiển thị dashboard.

Điểm cần nhấn mạnh khi trình bày:

- Kafka giúp tách producer và consumer, chịu tải tốt, có khả năng replay.
- Flink xử lý stateful streaming, checkpoint, watermark, event time và CEP.
- ClickHouse phù hợp cho truy vấn phân tích nhiều dòng dữ liệu với tốc độ cao.
- Grafana phục vụ quan sát hệ thống, không nằm trong đường quyết định giao dịch.
- MinIO đóng vai trò object storage nội bộ cho model và trạng thái Flink.

## 3. Kafka topics và ý nghĩa

Trong `docker-compose.yml`, `kafka-init` tạo:

```text
transactions  - 8 partitions  - chứa giao dịch đầu vào
fraud-rules   - 1 partition   - chứa luật gian lận dạng JSON
fraud-alerts  - 3 partitions  - chứa alert cuối cùng sau Decision Engine
```

`transactions` có 8 partitions vì đây là topic tải cao. Simulator gửi key là `account_id`, nhờ đó các event cùng tài khoản thường đi vào cùng Kafka partition. Sau đó Flink vẫn `keyBy(accountId)` để đảm bảo state hành vi được tính theo từng tài khoản.

`fraud-rules` chỉ có 1 partition vì lượng rule update thấp, cần thứ tự cập nhật đơn giản.

`fraud-alerts` là output topic, phục vụ mở rộng: notification service, case management, audit, core banking.

## 4. Data Simulator

File chính: `data-simulator/simulator.py`.

Simulator tạo dữ liệu giả lập nhưng có tính hành vi:

- 500 tài khoản: `ACC-000001` đến `ACC-000500`.
- 200 merchant: `MERCH-0001` đến `MERCH-0200`.
- Mỗi account có một card tương ứng.
- Channel gồm: `ATM`, `ONLINE`, `POS`, `MOBILE`.
- Event type gồm: `LOGIN`, `TRANSFER`, `PAYMENT`, `WITHDRAWAL`, `CHANGE_PASSWORD`, và một số event trong fraud sequence như `LOGIN_FAILED`, `LOGIN_SUCCESS`.
- Location gồm các thành phố Việt Nam và quốc tế: Ho Chi Minh, Hanoi, Da Nang, Singapore, Tokyo, New York, London, Sydney, Dubai, Seoul.

Trong Docker Compose hiện tại:

```yaml
EVENTS_PER_SECOND: 100
FRAUD_RATIO: 0.01
```

Nghĩa là simulator cố gắng sinh khoảng 100 event/giây, với xác suất khởi tạo fraud sequence khoảng 1%. Lưu ý: một fraud sequence có thể gồm nhiều event, nên tỷ lệ event fraud thực tế không nhất thiết đúng tuyệt đối 1%.

### 4.1. Schema event giao dịch

Mỗi event được tạo bởi `_make_event()` có dạng:

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

Ý nghĩa:

- `transaction_id`: định danh duy nhất.
- `account_id`: khóa để gom hành vi theo tài khoản.
- `card_id`: định danh thẻ.
- `timestamp`: thời điểm nghiệp vụ của event, dùng làm event time trong Flink.
- `produced_at`: thời điểm simulator gửi event, dùng tính end-to-end latency.
- `amount`: số tiền.
- `event_type`: loại hành vi/giao dịch.
- `channel`: kênh giao dịch.
- `location`: JSON string chứa latitude/longitude.
- `status`: trạng thái ban đầu.
- `merchant_id`: nơi phát sinh giao dịch.
- `is_fraud`: nhãn giả lập trong simulator.

Điểm cần nói trung thực: trong pipeline hiện tại, `is_fraud` từ simulator chưa được ghi nguyên vẹn vào bảng ClickHouse dưới dạng ground truth riêng. Trong `DecisionMaker`, field `is_fraud` của `DecisionResult` được set theo quyết định của engine. Vì vậy nếu muốn đánh giá Precision/Recall/FPR thật sự, nên bổ sung thêm cột như `ground_truth_is_fraud`.

### 4.2. Giao dịch bình thường

Hàm `generate_normal_transaction()`:

- Chọn home location của account ở một thành phố Việt Nam.
- Thêm nhiễu nhỏ quanh vị trí đó.
- Amount theo phân phối log-normal quanh mức trung bình của account.
- Event type chọn theo trọng số:
  - `PAYMENT`: 40%
  - `TRANSFER`: 30%
  - `WITHDRAWAL`: 20%
  - `LOGIN`: 10%
- `is_fraud = 0`.

Ý nghĩa: giao dịch bình thường có số tiền vừa phải, vị trí gần nơi thường trú, tần suất không quá bất thường.

### 4.3. Các loại fraud simulator tạo ra

Simulator có 5 kiểu bất thường.

1. Brute force/account takeover

Hàm: `generate_brute_force_sequence()`.

Luồng:

```text
3-5 LOGIN_FAILED
    -> LOGIN_SUCCESS
    -> TRANSFER số tiền 5,000 đến 50,000
```

Các failed login và login success có `is_fraud = 0`, giao dịch transfer cuối có `is_fraud = 1`.

Pattern này được CEP ATO bắt nếu có ít nhất 3 failed login, sau đó success, sau đó transfer trong 10 phút.

Lưu ý quan trọng: CEP ATO trong code set severity `CRITICAL` chỉ khi số failed login >= 7. Simulator hiện sinh 3-5 failed login, nên ATO thường ra severity `HIGH`, dẫn tới `ALERT` chứ không phải `BLOCK`, trừ khi ML hoặc rule khác đẩy lên block.

2. Rapid transactions

Hàm: `generate_rapid_transactions()`.

Luồng:

```text
3-6 giao dịch TRANSFER/PAYMENT
mỗi giao dịch 100 đến 2,000
cách nhau khoảng 500ms
```

Các event đều `is_fraud = 1` trong simulator. Tuy nhiên code CEP hiện tại không có pattern riêng tên velocity attack cho chuỗi 3-6 giao dịch này. Nó có thể bị ML đánh điểm cao do `tx_frequency_1h` tăng, nhưng không đảm bảo bị bắt bằng rule/CEP.

Đây là điểm có thể phản biện: hệ thống đã mô phỏng velocity attack, nhưng engine hiện phát hiện velocity chủ yếu gián tiếp qua feature/ML. Nếu muốn chắc hơn, có thể thêm CEP pattern riêng cho "N giao dịch trong T giây".

3. Impossible travel

Hàm: `generate_impossible_travel()`.

Luồng:

```text
Giao dịch 1 tại home location
Giao dịch 2 tại vị trí cách > 150km trong thời gian ngắn
```

Giao dịch thứ hai có `is_fraud = 1`. CEP pattern `IMPOSSIBLE_TRAVEL` sẽ tính khoảng cách Haversine giữa 2 tọa độ. Nếu khoảng cách >= 150km trong 15 phút thì tạo alert.

4. High amount

Hàm: `generate_high_amount_transaction()`.

Tạo một giao dịch đơn lẻ:

```text
amount từ 10,000 đến 100,000
event_type là TRANSFER/PAYMENT/WITHDRAWAL
```

Rule `RULE_001` bắt `amount > 10000` với severity `HIGH`. Nếu ML score cũng rất cao thì có thể `BLOCK`; nếu chỉ rule non-critical thì ra `ALERT`.

5. Micro transactions

Hàm: `generate_micro_transactions()`.

Luồng:

```text
5-8 giao dịch tài chính
mỗi giao dịch dưới 10
trong khoảng rất ngắn
```

CEP pattern `MICRO_TRANSACTIONS` bắt ít nhất 5 giao dịch dưới 10 trong 2 phút.

Severity phụ thuộc số giao dịch và độ đa dạng merchant:

- 1 merchant: `MEDIUM`.
- 2+ merchants: `HIGH`.
- 3+ merchants hoặc >= 10 giao dịch: `CRITICAL`.

Vì simulator chọn merchant ngẫu nhiên, nhiều trường hợp micro transactions sẽ có nhiều merchant và bị đẩy lên `CRITICAL`, dẫn tới `BLOCK`.

### 4.4. Initial rules do simulator gửi vào Kafka

Khi khởi động, simulator gọi `push_initial_rules()` và gửi 3 rule vào topic `fraud-rules`:

```json
{
  "rule_id": "RULE_001",
  "rule_name": "High Amount Threshold",
  "field": "amount",
  "operator": "GREATER_THAN",
  "threshold": 10000.0,
  "severity": "HIGH",
  "version": 1,
  "active": true
}
```

```json
{
  "rule_id": "RULE_002",
  "rule_name": "Blacklisted Merchant",
  "field": "merchant_id",
  "operator": "IN",
  "threshold": ["MERCH-0199", "MERCH-0198", "MERCH-0197"],
  "severity": "CRITICAL",
  "version": 1,
  "active": true
}
```

```json
{
  "rule_id": "RULE_003",
  "rule_name": "Nighttime Large Transfer",
  "field": "amount",
  "operator": "GREATER_THAN",
  "threshold": 5000.0,
  "time_window": {"start_hour": 0, "end_hour": 5},
  "severity": "MEDIUM",
  "version": 1,
  "active": true
}
```

Rule update được gửi qua Kafka nên có thể thay đổi rule runtime mà không cần restart Flink job.

## 5. Flink job tổng quan

File chính: `flink-job/src/main/java/com/frauddetection/FraudDetectionJob.java`.

Pipeline trong code:

```text
KafkaSource<Transaction>
    -> filter invalid transactions
    -> RulesEngine
    -> FraudPatternDetector.detectAll()
    -> CepAlertMerger
    -> FeatureAggregator
    -> OnnxModelScorer
    -> DecisionMaker
    -> ClickHouseSink
    -> AlertClickHouseSink
    -> KafkaSink<FraudAlert>
```

Parallelism:

```java
env.setParallelism(8);
```

Checkpoint:

```java
env.enableCheckpointing(60000);
mode = EXACTLY_ONCE;
minPauseBetweenCheckpoints = 30000;
checkpointTimeout = 120000;
maxConcurrentCheckpoints = 1;
enableUnalignedCheckpoints();
retain checkpoint on cancellation;
```

Trong Docker Compose, Flink config:

```yaml
state.backend: hashmap
state.checkpoints.dir: s3://flink-checkpoints/checkpoints
state.savepoints.dir: s3://flink-savepoints/savepoints
execution.checkpointing.interval: 60000
execution.checkpointing.mode: EXACTLY_ONCE
taskmanager.numberOfTaskSlots: 8
parallelism.default: 8
```

Điểm cần chú ý: README có nhắc RocksDB, nhưng cấu hình Docker Compose hiện tại dùng `state.backend: hashmap`. Project có dependency RocksDB, nhưng runtime hiện đang dùng HashMap State Backend. Khi phản biện, nên nói theo cấu hình thực tế: hiện tại dùng HashMap để đơn giản hóa demo, có thể chuyển sang RocksDB khi state lớn.

## 6. Event time, watermark và out-of-order

Transaction source:

```java
WatermarkStrategy
  .forBoundedOutOfOrderness(Duration.ofSeconds(5))
  .withTimestampAssigner((tx, ts) -> tx.getTimestamp())
  .withIdleness(Duration.ofSeconds(30));
```

Ý nghĩa:

- Flink dùng `timestamp` của transaction làm event time.
- Cho phép event đến trễ/tới không đúng thứ tự tối đa 5 giây.
- Nếu một partition Kafka không có dữ liệu trong 30 giây, Flink đánh dấu idle để watermark toàn cục không bị kẹt.

Tại sao cần watermark?

- CEP cần hiểu "trong 10 phút", "trong 2 phút", "trong 15 phút" theo thời gian sự kiện.
- Nếu chỉ dùng processing time, kết quả có thể sai khi dữ liệu đến muộn hoặc Kafka replay.

Rules source dùng watermark gần như tối đa (`Long.MAX_VALUE - 1000`) vì rule update không phụ thuộc event-time window.

## 7. Transaction deserialization

File: `TransactionDeserializer.java`.

Kafka message là JSON bytes. Deserializer:

1. Dùng Jackson chuyển JSON thành `Transaction`.
2. Gọi `tx.parseLocation()` để parse field `location`.
3. Nếu lỗi parse thì trả `null`.
4. Pipeline filter bỏ transaction null hoặc không có transaction id.

`Transaction.parseLocation()` dùng string parsing thủ công thay vì tạo ObjectMapper cho nested location ở mỗi event. Đây là tối ưu nhỏ để giảm overhead khi thông lượng cao.

## 8. Rules Engine

File: `RulesEngine.java`.

### 8.1. Vì sao dùng Broadcast State?

Rule là dữ liệu cấu hình nhỏ nhưng phải có mặt ở tất cả parallel subtasks. Nếu Flink job chạy parallelism 8, mọi subtask xử lý transaction đều cần cùng một bộ rule.

Broadcast State giải quyết việc này:

```text
fraud-rules topic
    -> broadcast stream
    -> mỗi task manager/subtask đều nhận rule update
    -> transaction nào đi qua cũng được evaluate bằng rule mới nhất
```

Ưu điểm:

- Cập nhật rule runtime.
- Không cần restart job.
- Không cần query database ngoài cho từng transaction.
- Độ trễ update rule thấp vì rule đi qua Kafka và được broadcast trong Flink.

### 8.2. Cách rule được cập nhật

`processBroadcastElement()`:

- Parse JSON thành `FraudRule`.
- Lấy `rule_id`.
- Kiểm tra version: nếu rule mới có version <= version cũ thì bỏ qua.
- Lưu raw JSON vào Broadcast State.
- Lưu object đã parse vào local cache `ruleCache` để tránh parse JSON lặp lại cho từng transaction.

Đây là thiết kế đúng cho hiệu năng: rule update ít, transaction nhiều. Ta chịu chi phí parse khi rule thay đổi, không chịu chi phí parse ở mọi event.

### 8.3. Cách transaction được evaluate

`processElement()`:

- Nếu cache rỗng sau restart, rebuild cache từ Broadcast State.
- Duyệt từng rule active.
- Lấy field từ transaction.
- So sánh theo operator.
- Kiểm tra thêm time window nếu rule có cấu hình.
- Nếu match, tạo `RuleViolation` và `FraudAlert` object.
- Gắn vào transaction:
  - `ruleAlerts`
  - `ruleAlertObjects`
  - `ruleFlagged = true`

Operator hỗ trợ:

```text
GREATER_THAN
LESS_THAN
EQUALS
IN
NOT_IN
```

Field hỗ trợ:

```text
amount
event_type
channel
merchant_id
status
card_id
account_id
```

Rule score:

```text
CRITICAL -> 0.8
khác CRITICAL -> 0.6
```

Time window:

- Dùng timezone `Asia/Ho_Chi_Minh`.
- Ví dụ `start_hour = 0`, `end_hour = 5` nghĩa là rule chỉ match từ 00:00 đến trước 05:00.

## 9. CEP Engine

File: `FraudPatternDetector.java`.

CEP là Complex Event Processing, dùng để phát hiện một chuỗi sự kiện theo thời gian, không chỉ một event đơn lẻ.

Input của CEP là stream đã qua Rules Engine và được `keyBy(accountId)`. Nghĩa là pattern được xét riêng cho từng tài khoản.

`detectAll()` tạo 3 stream alert:

```text
Account Takeover alerts
Micro Transactions alerts
Impossible Travel alerts
```

Sau đó union lại thành một stream `FraudAlert`.

Tất cả pattern dùng:

```java
AfterMatchSkipStrategy.skipPastLastEvent()
```

Ý nghĩa: sau khi match một pattern, các event đã match không bị dùng lại để tạo alert chồng chéo quá nhiều. Điều này giảm duplicate alert.

### 9.1. Pattern 1: Account Takeover

Logic:

```text
LOGIN_FAILED ít nhất 3 lần, tối đa 10 lần
    -> LOGIN_SUCCESS
    -> TRANSFER
trong 10 phút
```

Code:

```text
times(minFailed, 10).greedy()
followedBy("success")
followedBy("transfer")
within(10 minutes)
```

Default threshold:

```text
CEP_ATO_MIN_FAILED_LOGINS = 3
CEP_ATO_WINDOW_MINUTES = 10
```

Score:

```text
score = min(0.99, 0.80 + failedCount * 0.03)
```

Severity:

```text
failedCount >= 7 -> CRITICAL
ngược lại -> HIGH
```

Decision sau đó:

- `CRITICAL` CEP -> `BLOCK`.
- `HIGH` CEP -> `ALERT`.

### 9.2. Pattern 2: Micro Transactions

Logic:

```text
ít nhất 5 giao dịch tài chính
mỗi giao dịch amount < 10
trong 2 phút
```

Financial event types:

```text
TRANSFER
PAYMENT
WITHDRAWAL
```

Default threshold:

```text
CEP_MICRO_MIN_TRANSACTIONS = 5
CEP_MICRO_MAX_AMOUNT = 10.0
CEP_MICRO_WINDOW_MINUTES = 2
```

Score:

```text
score = min(0.98, 0.75 + count * 0.03)
```

Severity:

```text
count >= 10 hoặc distinctMerchants >= 3 -> CRITICAL
distinctMerchants <= 1 -> MEDIUM
còn lại -> HIGH
```

Ý nghĩa nghiệp vụ:

- Nhiều giao dịch nhỏ liên tiếp có thể là card testing, carding hoặc smurfing.
- Nếu cùng một merchant, có thể là hành vi mua hàng bình thường hơn.
- Nếu nhiều merchant, rủi ro cao hơn vì giống hành vi thử thẻ ở nhiều nơi.

### 9.3. Pattern 3: Impossible Travel

Logic:

```text
2 giao dịch tài chính cùng account
tọa độ hợp lệ
khoảng cách >= 150km
trong 15 phút
```

Default threshold:

```text
CEP_TRAVEL_DISTANCE_KM = 150.0
CEP_TRAVEL_WINDOW_MINUTES = 15
```

Khoảng cách dùng công thức Haversine:

```text
a = sin²(deltaLat/2) + cos(lat1) * cos(lat2) * sin²(deltaLon/2)
c = 2 * atan2(sqrt(a), sqrt(1-a))
distance = R * c
R = 6371 km
```

Score:

```text
score = min(0.99, 0.80 + distance / 10000.0)
```

Severity:

```text
distance >= 2000km -> CRITICAL
ngược lại -> HIGH
```

Ý nghĩa: nếu trong thời gian ngắn account giao dịch ở Việt Nam rồi ở London/New York, điều này không hợp lý về mặt vật lý.

## 10. CepAlertMerger

File: `CepAlertMerger.java`.

Vấn đề cần giải quyết:

- Transaction đi trên main stream rất nhanh.
- CEP branch cần thời gian pattern matching.
- Nếu main stream đi thẳng qua DecisionMaker trước khi alert CEP quay lại, transaction có thể bị `APPROVE` sai.

Giải pháp trong code:

- Buffer transaction trong `MapState`.
- Chờ một khoảng nhỏ `BUFFER_DELAY_MS = 10ms`.
- Nếu alert CEP đến trong lúc transaction còn buffer, gắn alert vào transaction.
- Nếu alert đến trước transaction, lưu vào `orphanAlerts`.
- Nếu alert đến sau khi transaction đã emit, tạo một bản transaction late alert và emit lại.

State trong merger:

- `bufferedTxns`: transaction đang chờ.
- `timerTxIds`: map timer timestamp -> danh sách transaction id.
- `orphanAlerts`: alert chưa tìm thấy transaction.
- `orphanIndex`: index phụ để lookup orphan alert theo transaction id.
- `emittedTxns`: transaction đã emit, dùng để xử lý alert đến muộn.

TTL:

- Buffered/emitted transaction: 60 giây.
- Orphan alert: 30 giây.

Điểm cần hiểu: nếu late CEP alert tạo thêm một bản transaction cùng `transaction_id`, ClickHouse `ReplacingMergeTree` theo `transaction_id` ở bảng transactions sẽ giúp giữ phiên bản mới hơn sau quá trình merge. Trong truy vấn cần độ chính xác tuyệt đối ngay lập tức, có thể dùng `FINAL` hoặc thiết kế sink idempotent mạnh hơn.

## 11. Feature Aggregator

File: `FeatureAggregator.java`.

FeatureAggregator tính các đặc trưng hành vi theo từng account để phục vụ ML.

Input được `keyBy(accountId)`, nên state là theo từng tài khoản.

Các feature:

```text
txCount1h        - số giao dịch trong 1 giờ gần nhất
txCount24h       - số giao dịch trong 24 giờ gần nhất
avgAmount24h     - số tiền trung bình 24 giờ gần nhất
amountDeviation  - độ lệch amount hiện tại so với trung bình 24h
timeSinceLastMs  - thời gian từ giao dịch trước
distanceKm       - khoảng cách từ vị trí giao dịch trước
```

### 11.1. State được dùng

ValueState:

- `count1hState`
- `count24hState`
- `sumAmount24hState`
- `sumSquared24hState`
- `lastTxTime`
- `lastLatitudeState`
- `lastLongitudeState`

MapState:

- `timer1hIndex`: timer timestamp -> danh sách amount cần xóa khỏi cửa sổ 1h.
- `timer24hIndex`: timer timestamp -> danh sách amount cần xóa khỏi cửa sổ 24h.

Tất cả state có TTL để tránh tăng vô hạn khi account không còn hoạt động.

### 11.2. Công thức tính feature

Các feature được tính trước khi update state bằng giao dịch hiện tại. Nhờ vậy feature phản ánh lịch sử trước giao dịch hiện tại.

```text
avgAmount24h = sumAmount24h / count24h
```

Nếu có ít nhất 2 giao dịch trong 24h:

```text
variance = (sumSquared24h / count24h) - avgAmount24h²
stdDev = sqrt(max(0, variance))
amountDeviation = (currentAmount - avgAmount24h) / stdDev
```

Nếu `stdDev = 0`, `amountDeviation = 0`.

```text
timeSinceLastMs = max(0, currentTimestamp - lastTxTimestamp)
```

```text
distanceKm = Haversine(lastLat, lastLon, currentLat, currentLon)
```

Sau khi tính xong, aggregator update:

```text
count1h += 1
count24h += 1
sumAmount24h += amount
sumSquared24h += amount * amount
lastTxTime = current timestamp nếu mới hơn
lastLatitude/Longitude = tọa độ hiện tại
```

### 11.3. Vì sao không quét toàn bộ lịch sử?

Thiết kế cũ kiểu "lưu danh sách toàn bộ giao dịch rồi mỗi event quét lại" sẽ tốn O(N) mỗi transaction.

Thiết kế hiện tại dùng counter cộng dồn:

- Mỗi event đọc state O(1).
- Update counter O(1).
- Timer xử lý giảm counter khi event ra khỏi cửa sổ.

Vì vậy chi phí trung bình mỗi giao dịch nhỏ hơn nhiều, phù hợp stream realtime.

### 11.4. Vì sao dùng processing-time timer?

Code có comment rõ: event-time timer chỉ fire khi watermark vượt qua timestamp timer. Khi throughput thấp hoặc không đều, watermark có thể chậm, làm state không được cleanup đúng lúc.

Processing-time timer dựa trên thời gian thực của máy, nên việc giảm counter sau 1h/24h ổn định hơn trong demo.

Đây là trade-off:

- Processing-time timer ổn định hơn về cleanup/runtime.
- Event-time timer chính xác hơn nếu replay dữ liệu lịch sử.

Với demo realtime đang chạy liên tục, lựa chọn processing-time là hợp lý.

### 11.5. Short-circuit CRITICAL

Nếu transaction đã có alert `CRITICAL` từ Rules hoặc CEP:

- FeatureAggregator không tính feature.
- Gắn feature rỗng.
- Emit tiếp.

Lý do:

- Transaction đã đủ điều kiện block, ML không cần thiết.
- Tránh giao dịch gian lận cực đoan làm bẩn profile hành vi của account.
- Giảm CPU và state update.

## 12. ML model training

File: `ml-model/train_model.py`.

Luồng trainer:

```text
generate synthetic data
    -> train RandomForestClassifier
    -> evaluate AUC-ROC, AUC-PR
    -> export ONNX
    -> upload to MinIO bucket ml-models
```

Feature dùng để train:

```text
[amount, hour_of_day, is_foreign, tx_frequency_1h, distance_km]
```

Đây phải khớp với feature vector mà Flink gửi vào ONNX.

Synthetic dataset:

- 50,000 samples.
- 97% legitimate.
- 3% fraud.

Legitimate data:

- Amount log-normal, clip 1 đến 10,000.
- Hour từ 6 đến 22.
- Foreign probability khoảng 5%.
- Frequency theo Poisson trung bình 2.
- Distance nhỏ, thường dưới 100km.

Fraud data:

- Amount cao hơn, clip 500 đến 100,000.
- Hour thường từ 0 đến 5 hoặc 23.
- Foreign probability khoảng 70%.
- Frequency theo Poisson trung bình 8.
- Distance lớn, có thể đến 20,000km.

Model:

```text
RandomForestClassifier
n_estimators = 100
max_depth = 10
min_samples_split = 5
min_samples_leaf = 2
class_weight = balanced
```

Vì dữ liệu fraud chỉ 3%, dùng `class_weight = balanced` để giảm bias về class legitimate.

Export:

- Dùng `skl2onnx`.
- `target_opset = 12`.
- `zipmap = False`, để output probability là mảng, dễ đọc trong Java.

Upload:

- File `fraud_detector.onnx` được upload lên `s3://ml-models/fraud_detector.onnx` trong MinIO.

## 13. ONNX Model Scorer trong Flink

File: `OnnxModelScorer.java`.

### 13.1. Cách model được load

Trong `open()` của Flink RichFlatMapFunction:

1. Kết nối MinIO.
2. Download `fraud_detector.onnx` về file tạm.
3. Tạo ONNX Runtime session.
4. Lấy input name của model.
5. Tạo buffer `float[1][5]`.
6. Xóa file tạm sau khi model đã load.

Điểm mạnh:

- Model được load một lần cho mỗi operator subtask, không download mỗi transaction.
- Inference chạy trực tiếp trong JVM, không gọi HTTP sang model server.
- Giảm network latency và giảm điểm lỗi vận hành.

### 13.2. Feature vector khi scoring

Flink extract:

```text
buf[0] = amount
buf[1] = hour_of_day theo Asia/Ho_Chi_Minh
buf[2] = is_foreign
buf[3] = tx_frequency_1h
buf[4] = distance_km
```

`is_foreign`:

```text
0 nếu latitude trong [8.0, 23.5] và longitude trong [102.0, 110.0]
1 nếu nằm ngoài bounding box Việt Nam
```

`tx_frequency_1h` lấy từ `AggregateFeatures.txCount1h`.

`distance_km` lấy từ `AggregateFeatures.distanceKm`.

### 13.3. Output ML score

ONNX Runtime trả output. Code ưu tiên đọc probability:

```text
fraudProb = probabilities[0][1]
```

Sau đó:

```text
tx.mlScore = round(fraudProb, 6 chữ số)
tx.mlAvailable = true
```

Nếu model không load được hoặc inference lỗi:

```text
mlScore = 0.0
mlAvailable = false
```

Nhờ đó pipeline không chết toàn bộ vì lỗi ML, nhưng chất lượng phát hiện sẽ giảm.

### 13.4. Short-circuit CRITICAL

Nếu transaction đã CRITICAL từ Rules/CEP:

- Không chạy ONNX inference.
- `mlScore = 0`.
- `mlAvailable = false`.

Lý do: Decision Engine chắc chắn sẽ `BLOCK`, nên ML không ảnh hưởng kết quả.

## 14. Decision Engine

File: `DecisionMaker.java`.

DecisionMaker nhận transaction đã được enrich:

- Rule violations.
- CEP alerts.
- Aggregate features.
- ML score.

Nó xuất `DecisionResult`, là record ghi vào ClickHouse.

### 14.1. Combined score

Code tính:

```text
maxCepScore = max(score của CEP alerts)
maxRuleScore = max(score của rule violations)
combinedScore = max(mlScore, maxCepScore, maxRuleScore)
```

Combined score không phải trung bình. Nó lấy rủi ro cao nhất trong 3 nguồn.

Lý do hợp lý:

- Nếu một engine rất chắc chắn, không nên bị làm loãng bởi engine khác.
- Rule CRITICAL hoặc CEP CRITICAL có thể đủ để block dù ML thấp.

### 14.2. Logic quyết định thực tế trong code

Thứ tự hiện tại:

```text
1. Nếu có CRITICAL rule
      -> BLOCK, source = RULES_ENGINE

2. Nếu có CRITICAL CEP
      -> BLOCK, source = CEP_ENGINE

3. Nếu có CEP non-critical
      -> ALERT, source = CEP_ENGINE

4. Nếu mlScore > 0.85
      -> BLOCK, source = ML_SCORING

5. Nếu mlScore > 0.5
      -> ALERT, source = ML_SCORING

6. Nếu có rule non-critical
      -> ALERT, source = RULES_ENGINE

7. Nếu không có gì bất thường
      -> APPROVE, source = NONE
```

Điểm cần nói chính xác: README mô tả ngắn là "Rules > CEP > ML", nhưng code hiện tại ưu tiên CRITICAL rule trước, rồi CEP, rồi ML, rồi mới non-critical rule. Vì vậy nếu một rule `HIGH` match nhưng ML score > 0.85, quyết định cuối là `BLOCK` do ML.

Threshold ML:

```text
mlScore > 0.85 -> BLOCK
mlScore > 0.50 -> ALERT
```

Severity mapping:

- Rule CRITICAL: block ngay.
- CEP CRITICAL: block ngay.
- CEP HIGH/MEDIUM: alert.
- ML cao: block/alert theo threshold.
- Rule HIGH/MEDIUM/LOW: alert nếu chưa bị ML quyết định trước.

### 14.3. Output DecisionResult

Các field quan trọng:

```text
transaction_id
account_id
card_id
event_time
amount
event_type
channel
latitude
longitude
merchant_id
status = COMPLETED
ml_score
rule_triggered
is_fraud
decision
decision_source
combined_score
produced_at
decided_at
```

`decided_at = System.currentTimeMillis()`, dùng để tính latency:

```text
latency_ms = decided_at(Thời điểm Flink xử lý xong) - produced_at(Thời điểm event được đẩy vào Kafka)
```

## 15. Alert output

Sau DecisionMaker, pipeline tạo `allAlerts` từ `DecisionResult`.

Điều kiện:

```text
decision == ALERT hoặc decision == BLOCK
```

Alert type:

```text
FINAL_ALERT
FINAL_BLOCK
```

Severity:

```text
BLOCK -> CRITICAL
ALERT -> HIGH
```

Source:

- Nếu decision source là `CEP_ENGINE`, alert source thành `CEP`.
- Nếu source khác, giữ `RULES_ENGINE` hoặc `ML_SCORING`.

Alert được ghi vào:

- ClickHouse table `fraud_detection.fraud_alerts`.
- Kafka topic `fraud-alerts`.

Điểm cần hiểu: bảng `fraud_alerts` hiện lưu final alerts sau Decision Engine, không ghi trực tiếp toàn bộ raw CEP alert/rule alert. Điều này giúp dashboard nhìn theo quyết định cuối cùng, nhưng nếu muốn audit chi tiết từng rule/CEP raw alert thì nên bổ sung bảng riêng.

## 16. ClickHouse storage

File: `clickhouse/init-db.sql`.

Database:

```sql
CREATE DATABASE IF NOT EXISTS fraud_detection;
```

### 16.1. Bảng transactions

Lưu kết quả cuối cùng của mọi giao dịch:

```sql
fraud_detection.transactions
```

Engine:

```sql
ReplacingMergeTree(decided_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (transaction_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY
```

Ý nghĩa:

- Partition theo ngày event_time giúp truy vấn theo thời gian nhanh hơn.
- `ORDER BY transaction_id` giúp deduplicate theo transaction id.
- `ReplacingMergeTree(decided_at)` giữ version mới hơn khi có duplicate.
- TTL 90 ngày tự dọn dữ liệu cũ.

Caveat: ReplacingMergeTree dedup theo cơ chế merge nền, không phải lúc nào cũng loại duplicate ngay lập tức trong mọi truy vấn. Khi cần kết quả chính xác tại thời điểm truy vấn, có thể dùng `FINAL`, đổi mô hình bảng, hoặc thiết kế sink idempotent hơn.

### 16.2. Bảng fraud_alerts

Lưu alert cuối cùng:

```sql
fraud_detection.fraud_alerts
```

Engine:

```sql
ReplacingMergeTree(alert_time)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (alert_id)
TTL toDateTime(alert_time) + INTERVAL 180 DAY
```

TTL alert là 180 ngày vì alert thường phục vụ audit lâu hơn transaction thường.

### 16.3. Materialized views

ClickHouse tạo sẵn các bảng aggregate:

1. `tx_throughput`

Tính số giao dịch, tổng amount, fraud count theo phút.

2. `alerts_by_type`

Tính số alert và average score theo ngày, alert type và source.

3. `fraud_by_geo`

Gom fraud theo bucket latitude/longitude đã làm tròn.

4. `latency_stats`

Tính latency trung bình, latency max và count theo phút.

Latency:

```sql
dateDiff('millisecond', produced_at, decided_at)
```

## 17. ClickHouse sinks trong Flink

### 17.1. Transaction sink

File: `ClickHouseSink.java`.

Pipeline gọi:

```java
new ClickHouseSink(
  "fraud_detection.transactions",
  500,
  2000,
  CLICKHOUSE_HOST,
  CLICKHOUSE_PORT,
  CLICKHOUSE_USER,
  CLICKHOUSE_PASSWORD
)
```

Nghĩa là:

- Batch tối đa 500 record.
- Flush interval 2000ms.
- Ghi bằng HTTP `INSERT ... FORMAT JSONEachRow`.

Thiết kế:

- Buffer trong memory.
- Flush bằng background thread để không block operator thread.
- `snapshotState()` lưu buffer và inflight records vào Flink state.
- Nếu flush lỗi, re-add vào buffer để retry, có giới hạn để tránh tăng vô hạn.

### 17.2. Alert sink

File: `AlertClickHouseSink.java`.

Config:

```text
MAX_BATCH_SIZE = 100
FLUSH_INTERVAL_MS = 3000
```

Cũng dùng async HTTP flush và checkpoint state tương tự transaction sink.

### 17.3. Exactly-once hay near exactly-once?

Flink source và state có checkpoint `EXACTLY_ONCE`. Kafka offset được commit theo checkpoint.

Tuy nhiên ClickHouse sink không phải TwoPhaseCommit sink chuẩn. Nó dùng:

- checkpoint state để tránh mất record trong buffer/inflight;
- retry khi flush lỗi;
- ReplacingMergeTree để deduplicate duplicate records.

Vì vậy nên trình bày là:

```text
Hệ thống đạt near exactly-once cho end-to-end ClickHouse sink.
Flink state/source là exactly-once, còn ClickHouse đạt idempotent/dedup theo thiết kế bảng.
```

Đừng nói tuyệt đối "exactly-once end-to-end" nếu hội đồng hỏi sâu về sink.

## 18. Grafana dashboard và các chỉ số

File: `grafana/provisioning/dashboards/fraud-dashboard.json`.

Datasource:

```yaml
server: clickhouse
port: 8123
defaultDatabase: fraud_detection
username: default
password: clickhouse123
```

### 18.1. Total Transactions

```sql
SELECT count() as total
FROM fraud_detection.transactions
```

Tổng số giao dịch đã được DecisionMaker ghi vào ClickHouse.

### 18.2. Fraud Detected

```sql
SELECT count() as fraud
FROM fraud_detection.transactions
WHERE is_fraud = 1
```

Trong code hiện tại, `is_fraud = 1` nghĩa là engine quyết định giao dịch là fraud (`ALERT` hoặc `BLOCK`), không phải ground truth gốc từ simulator.

### 18.3. Fraud Rate

```sql
SELECT round(countIf(is_fraud = 1) * 100.0 / count(), 2) as fraud_rate
FROM fraud_detection.transactions
```

Công thức:

```text
fraud_rate = số giao dịch bị engine đánh fraud / tổng giao dịch * 100%
```

### 18.4. BLOCKED Transactions

```sql
SELECT count() as blocked
FROM fraud_detection.transactions
WHERE decision = 'BLOCK'
```

Đếm giao dịch bị chặn.

### 18.5. Avg ML Score

```sql
SELECT round(avg(ml_score), 4) as avg_ml
FROM fraud_detection.transactions
WHERE ml_score > 0
```

Chỉ tính những giao dịch có ML score > 0. Giao dịch CRITICAL bị short-circuit sẽ có `ml_score = 0` và không phản ánh trong chỉ số này.

### 18.6. Total Alerts

```sql
SELECT uniqExact(alert_id) as alerts
FROM fraud_detection.fraud_alerts
```

Dung `uniqExact(alert_id)` de dashboard khong dem duplicate trong luc ClickHouse chua merge xong cac ban ghi `ReplacingMergeTree`.

Đếm final alerts.

### 18.7. Throughput

```sql
SELECT round(count() / 60.0, 1) AS events_per_sec
FROM fraud_detection.transactions
WHERE processing_time >= now() - INTERVAL 1 MINUTE
```

Công thức:

```text
events_per_sec = số record ghi vào ClickHouse trong 1 phút gần nhất / 60
```

Đây là throughput quan sát ở sink ClickHouse, không phải throughput thô ở Kafka.

### 18.8. End-to-End Latency

```sql
SELECT round(avg(dateDiff('millisecond', produced_at, decided_at)), 0) AS avg_latency_ms
FROM fraud_detection.transactions
WHERE produced_at > '1970-01-02'
  AND decided_at > '1970-01-02'
  AND processing_time >= now() - INTERVAL 5 MINUTE
```

Công thức:

```text
latency_ms = decided_at - produced_at
```

Bao gồm:

- thời gian event nằm trong Kafka;
- thời gian Flink xử lý Rules/CEP/ML/Decision;
- độ trễ do buffer CEP 10ms;
- không nhất thiết bao gồm toàn bộ thời gian visible trên Grafana, vì Grafana query theo dữ liệu đã vào ClickHouse.

Dashboard còn có p95/p99:

```sql
quantile(0.95)(dateDiff('millisecond', produced_at, decided_at))
quantile(0.99)(dateDiff('millisecond', produced_at, decided_at))
```

### 18.9. FDR và FPR trên dashboard

Dashboard hiện có:

```sql
SELECT round(
  countIf(is_fraud = 1 AND decision IN ('BLOCK', 'ALERT')) * 100.0 /
  greatest(countIf(is_fraud = 1), 1),
  2
) AS fdr
FROM fraud_detection.transactions
```

```sql
SELECT round(
  countIf(is_fraud = 0 AND decision = 'BLOCK') * 100.0 /
  greatest(countIf(is_fraud = 0), 1),
  2
) AS fpr
FROM fraud_detection.transactions
```

Điểm cần phản biện trung thực:

- Công thức FDR/FPR đúng nghĩa cần ground truth.
- Nhưng bảng `transactions` hiện không lưu ground truth gốc từ simulator.
- `is_fraud` trong bảng hiện là output của DecisionMaker.
- Vì vậy dashboard hiện đang thể hiện consistency của decision output, chưa phải metric đánh giá ML/rule so với nhãn thật.

Định nghĩa đúng nếu có ground truth:

```text
TP = ground_truth = fraud và decision in (ALERT, BLOCK)
FP = ground_truth = legit và decision in (ALERT, BLOCK)
TN = ground_truth = legit và decision = APPROVE
FN = ground_truth = fraud và decision = APPROVE

Precision = TP / (TP + FP)
Recall/FDR = TP / (TP + FN)
FPR = FP / (FP + TN)
F1 = 2 * Precision * Recall / (Precision + Recall)
```

Nếu hội đồng hỏi, câu trả lời nên là:

```text
Trong demo hiện tại, dashboard dùng nhãn phát hiện cuối cùng để quan sát vận hành.
Để đánh giá mô hình/engine nghiêm ngặt, em sẽ tách thêm cột ground_truth_is_fraud từ simulator và tính TP/FP/TN/FN theo ground truth đó.
```

### 18.10. Alert and decision breakdown

Dashboard có:

```sql
SELECT alert_source, count() AS cnt
FROM (
  SELECT alert_id, argMax(alert_source, alert_time) AS alert_source
  FROM fraud_detection.fraud_alerts
  GROUP BY alert_id
)
GROUP BY alert_source
ORDER BY cnt DESC
```

```sql
SELECT alert_type, count() AS count
FROM (
  SELECT alert_id, argMax(alert_type, alert_time) AS alert_type
  FROM fraud_detection.fraud_alerts
  GROUP BY alert_id
)
GROUP BY alert_type
ORDER BY count DESC
LIMIT 10
```

```sql
SELECT decision, count() AS cnt
FROM fraud_detection.transactions
GROUP BY decision
```

```sql
SELECT decision_source, count() AS cnt
FROM fraud_detection.transactions
WHERE decision_source != ''
  AND decision_source != 'NONE'
GROUP BY decision_source
ORDER BY cnt DESC
```

Các query này giúp trả lời:

- Bao nhiêu giao dịch approve/alert/block?
- Engine nào phát hiện nhiều nhất: Rules, CEP hay ML?
- Loại alert nào phổ biến?

## 19. Luồng khởi động Docker Compose

Lệnh chạy:

```bash
docker compose up --build -d
```

Thứ tự logic:

1. Kafka start và healthcheck.
2. `kafka-init` tạo topic.
3. MinIO start và healthcheck.
4. `minio-init` tạo bucket.
5. ClickHouse start, chạy `init-db.sql`.
6. Flink JobManager và TaskManager start.
7. `ml-trainer` train model và upload ONNX vào MinIO.
8. `flink-job-submitter` đợi Flink sẵn sàng, sleep 30s, submit JAR:

```bash
/opt/flink/bin/flink run \
  --jobmanager flink-jobmanager:8081 \
  -c com.frauddetection.FraudDetectionJob \
  /opt/flink/jobs/fraud-detection-job-1.0.0.jar
```

9. `data-simulator` bắt đầu gửi rule và transaction.
10. Grafana đọc ClickHouse.

Các URL:

```text
Grafana:       http://localhost:3000  admin/admin
Flink UI:      http://localhost:8081
MinIO Console: http://localhost:9001  admin/password123
ClickHouse:    http://localhost:8123  default/clickhouse123
```

Lệnh kiểm tra:

```bash
docker compose ps
docker compose logs -f flink-job-submitter
docker compose logs -f data-simulator
docker compose logs -f flink-taskmanager
```

Truy vấn nhanh:

```bash
docker exec clickhouse clickhouse-client \
  --query "SELECT count() FROM fraud_detection.transactions"
```

```bash
docker exec clickhouse clickhouse-client \
  --query "SELECT decision, decision_source, count() FROM fraud_detection.transactions GROUP BY decision, decision_source"
```

```bash
docker exec clickhouse clickhouse-client \
  --query "SELECT avg(dateDiff('millisecond', produced_at, decided_at)) FROM fraud_detection.transactions WHERE produced_at > '1970-01-02'"
```

## 20. Vì sao thiết kế này phù hợp realtime fraud detection?

### Kafka

- Chịu tải ghi cao.
- Buffer giữa simulator và Flink.
- Có partition theo account.
- Có thể replay dữ liệu nếu cần debug.
- Cho phép nhiều consumer group đọc cùng dữ liệu.

### Flink

- Xử lý stream stateful.
- Hỗ trợ event time/watermark.
- Hỗ trợ CEP.
- Checkpoint cho fault tolerance.
- Keyed state giúp tính hành vi theo từng account.
- Broadcast state giúp update rule động.

### ONNX Runtime trong Flink

- Không cần model serving service riêng.
- Không mất network round trip.
- Model được load một lần.
- Dễ đóng gói cùng Flink job.

Trade-off:

- Muốn update model phải upload model mới và restart/savepoint job hoặc bổ sung cơ chế reload.
- Nếu model lớn, memory mỗi parallel subtask tăng.

### ClickHouse

- Columnar database tối ưu cho analytics.
- Query count/group by/time series nhanh.
- Materialized view giúp pre-aggregate.
- ReplacingMergeTree hỗ trợ dedup gần idempotent.

### Grafana

- Dashboard realtime.
- Dễ trình bày throughput, latency, fraud rate, decision breakdown.
- Không ảnh hưởng logic xử lý chính.

## 21. Các điểm mạnh khi bảo vệ

1. Có nhiều lớp phát hiện

Hệ thống không phụ thuộc hoàn toàn vào ML. Rule bắt trường hợp rõ ràng, CEP bắt chuỗi hành vi, ML bắt pattern xác suất.

2. Có explainability

Decision result có:

- `decision`
- `decision_source`
- `rule_triggered`
- `combined_score`
- `ml_score`

Nên có thể giải thích giao dịch bị block do đâu.

3. Rule update động

Rule đi qua Kafka `fraud-rules` và Broadcast State, không cần redeploy.

4. Stateful realtime

FeatureAggregator và CEP đều dùng state theo account, phù hợp fraud detection vì gian lận thường là hành vi theo chuỗi, không chỉ một record đơn lẻ.

5. Có fault tolerance

Flink checkpoint 60s, externalized checkpoint, state checkpoint vào MinIO.

6. Có monitoring

Grafana hiển thị throughput, latency, alert breakdown, decision source, fraud amount.

7. Có khả năng mở rộng

Kafka partitions 8, Flink parallelism 8, TaskManager slots 8. Có thể tăng partitions và parallelism nếu tải lớn.

## 22. Hạn chế hiện tại và cách trả lời

### 22.1. Dữ liệu là synthetic

Hạn chế:

- Simulator chưa phản ánh đầy đủ hành vi thật.
- Phân phối fraud/legit do ta tự thiết kế.

Cách trả lời:

```text
Mục tiêu project là chứng minh kiến trúc realtime streaming và cơ chế phát hiện đa lớp.
Với dữ liệu production, phần simulator sẽ được thay bằng core banking/card transaction stream.
Model cũng sẽ được train lại bằng lịch sử fraud thật và kiểm định bằng ground truth.
```

### 22.2. FDR/FPR dashboard chưa phải evaluation chuẩn

Hạn chế:

- Bảng ClickHouse chưa lưu `ground_truth_is_fraud`.
- `is_fraud` hiện là output của engine.

Cách trả lời:

```text
Em phân biệt operational metric và model evaluation metric.
Dashboard hiện thiên về monitoring vận hành.
Để đánh giá chuẩn, cần persist thêm nhãn ground truth từ simulator hoặc từ hệ thống chargeback/investigation.
Sau đó tính TP/FP/TN/FN chuẩn.
```

### 22.3. Exactly-once với ClickHouse là near exactly-once

Hạn chế:

- ClickHouse sink custom không phải 2PC sink.
- Dedup phụ thuộc ReplacingMergeTree.

Cách trả lời:

```text
Flink source và state chạy exactly-once theo checkpoint.
Đối với ClickHouse, em thiết kế idempotent bằng transaction_id và ReplacingMergeTree, nên đạt near exactly-once.
Nếu production yêu cầu tuyệt đối hơn, em sẽ dùng sink transactional/idempotent nghiêm ngặt hơn hoặc staging table kèm merge theo transaction_id.
```

### 22.4. Single Kafka broker

Hạn chế:

- Docker Compose demo dùng 1 broker, replication factor 1.

Cách trả lời:

```text
Đây là cấu hình local demo để giảm tài nguyên.
Production cần ít nhất 3 brokers, replication factor >= 3, min ISR phù hợp, bật security và monitoring.
```

### 22.5. State backend hiện là HashMap

Hạn chế:

- HashMap backend phù hợp demo, nhưng state lớn có thể gây áp lực memory.

Cách trả lời:

```text
Với dữ liệu lớn, em sẽ chuyển sang RocksDB state backend hoặc EmbeddedRocksDBStateBackend để state nằm trên disk/local SSD và checkpoint incremental tốt hơn.
```

### 22.6. Model chưa reload động

Hạn chế:

- ONNX model load trong `open()`.
- Muốn cập nhật model cần restart job hoặc bổ sung cơ chế reload.

Cách trả lời:

```text
Thiết kế hiện tại tối ưu latency inference.
Phiên bản production có thể thêm model registry/versioning, broadcast model metadata, hoặc reload model theo timer/savepoint.
```

## 23. Câu hỏi phản biện thường gặp

### Vì sao không dùng batch processing?

Fraud detection cần phản ứng ngay khi giao dịch xảy ra. Batch có độ trễ phút/giờ, không phù hợp để block giao dịch realtime. Stream processing cho phép alert/block trong vài trăm ms đến vài giây.

### Vì sao vừa dùng rule vừa dùng ML?

Rule dễ giải thích và bắt case rõ ràng như blacklist, high amount. ML bắt pattern phức tạp hơn nhưng khó giải thích hơn. Kết hợp giúp cân bằng precision, recall và explainability.

### Vì sao cần CEP?

Nhiều fraud không thể phát hiện bằng một event đơn lẻ. Ví dụ account takeover cần chuỗi failed login -> login success -> transfer. CEP sinh ra để bắt các pattern theo thứ tự và trong time window.

### Vì sao keyBy accountId?

Fraud hành vi thường gắn với một account/card. Key by account giúp mọi event của cùng account vào cùng logical state, từ đó tính rolling count, last location, CEP pattern chính xác.

### Vì sao watermark 5 giây?

Kafka/event stream có thể out-of-order. Watermark 5 giây là trade-off giữa độ chính xác event-time và latency. Chờ quá lâu thì latency tăng, chờ quá ít thì dễ miss event đến muộn.

### Vì sao rule stream starting offset earliest còn transaction latest?

Rule cần đọc từ đầu để Flink có bộ rule mới nhất khi job start/restart. Transaction dùng latest để demo không xử lý lại backlog cũ khi start mới. Nếu cần replay, có thể đổi transaction source sang earliest hoặc offset cụ thể.

### Nếu model fail thì hệ thống có dừng không?

Không. `OnnxModelScorer` nếu không load được model thì set `mlAvailable=false`, `mlScore=0` và vẫn emit transaction. Rules và CEP vẫn hoạt động.

### Nếu ClickHouse chậm thì sao?

Sink flush async bằng background thread để giảm backpressure lên Flink operator. Buffer/inflight records được checkpoint. Tuy nhiên nếu ClickHouse lỗi kéo dài, buffer có giới hạn và có thể drop để tránh memory tăng vô hạn. Production cần queue/sink bền hơn và alert vận hành.

### Vì sao dùng MinIO?

MinIO cung cấp S3-compatible storage local:

- Lưu ONNX model cho Flink tải.
- Lưu Flink checkpoints/savepoints.
- Dễ thay bằng AWS S3 hoặc object storage thật trong production.

### Vì sao ClickHouse không phải PostgreSQL?

Dashboard chủ yếu count, group by, aggregate theo thời gian trên rất nhiều record. ClickHouse là columnar OLAP database, phù hợp hơn PostgreSQL cho workload analytics realtime.

### Vì sao alert cuối mới ghi vào fraud_alerts?

Để bảng alert phản ánh quyết định cuối sau khi đã resolve conflict giữa Rules, CEP và ML. Nếu cần audit chi tiết, có thể thêm bảng raw alerts để lưu tất cả alert trung gian.

## 24. Gợi ý cải tiến nếu có thời gian

Ưu tiên cao:

- Thêm `ground_truth_is_fraud` vào `DecisionResult` và ClickHouse để tính Precision/Recall/FPR thật.
- Thêm CEP velocity attack riêng cho rapid transactions.
- Ghi raw rule/CEP alerts vào bảng audit riêng.
- Cấu hình KafkaSink `fraud-alerts` với delivery guarantee rõ ràng hơn.
- Dùng RocksDB state backend cho state lớn.

Ưu tiên trung bình:

- Thêm Schema Registry hoặc JSON schema validation.
- Thêm rule management API/UI.
- Thêm Prometheus + Flink metrics.
- Thêm model version vào DecisionResult.
- Thêm model drift monitoring.

Production hardening:

- Kafka cluster 3 brokers, replication factor 3.
- TLS/SASL cho Kafka.
- Auth/secret management thay vì password trong compose.
- ClickHouse cluster/replication.
- CI/CD deployment với savepoint.
- Canary deployment cho model/rule.

## 25. Tóm tắt 1 phút để trình bày

```text
Hệ thống của em là nền tảng phát hiện gian lận giao dịch theo thời gian thực.
Data Simulator sinh giao dịch và rule update vào Kafka.
Flink đọc stream giao dịch, xử lý theo event time với watermark 5 giây, sau đó chạy qua 3 lớp phát hiện:
Rules Engine dùng Broadcast State để cập nhật luật động,
CEP Engine bắt các chuỗi hành vi như account takeover, micro transactions và impossible travel,
ML Scoring dùng model Random Forest export ONNX và chạy trực tiếp trong JVM.
Decision Engine gom kết quả theo priority, xuất APPROVE, ALERT hoặc BLOCK.
Kết quả được ghi vào ClickHouse bằng batch async sink và hiển thị trên Grafana.
Hệ thống có checkpoint, state theo account, dashboard latency/throughput và có thể mở rộng alert sang SMS, email, case management hoặc core banking.
```

## 26. Tóm tắt logic quyết định bằng ví dụ

Ví dụ 1: merchant nằm trong blacklist

```text
merchant_id = MERCH-0199
RULE_002 match, severity CRITICAL
Decision = BLOCK
Decision source = RULES_ENGINE
```

Ví dụ 2: 5 giao dịch nhỏ dưới 10 trong 2 phút, nhiều merchant

```text
CEP MICRO_TRANSACTIONS match
distinctMerchants >= 3
severity = CRITICAL
Decision = BLOCK
Decision source = CEP_ENGINE
```

Ví dụ 3: amount = 15,000, không có CEP, ML score = 0.4

```text
RULE_001 match, severity HIGH
ML không vượt threshold
Decision = ALERT
Decision source = RULES_ENGINE
```

Ví dụ 4: amount bình thường nhưng ML score = 0.91

```text
No critical rule
No CEP
ML score > 0.85
Decision = BLOCK
Decision source = ML_SCORING
```

Ví dụ 5: giao dịch bình thường

```text
No rule
No CEP
ML score <= 0.5
Decision = APPROVE
Decision source = NONE
```
