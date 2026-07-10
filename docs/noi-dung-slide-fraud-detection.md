# Nội dung trình bày slide: Real-Time Fraud Detection Platform

Số lượng đề xuất: 28 slide  
Thời lượng phù hợp: 18-25 phút  
Mục tiêu: trình bày được bài toán, kiến trúc, luồng xử lý, 3 lớp phát hiện gian lận, dashboard, điểm mạnh, hạn chế và hướng phát triển.

## Gợi ý phong cách thiết kế

- Tông màu: xanh navy, trắng, xám nhạt, điểm nhấn đỏ/cam cho cảnh báo gian lận.
- Font: dùng một font sans-serif rõ ràng, ví dụ Inter, Aptos, Calibri hoặc Roboto.
- Mỗi slide chỉ nên có 3-5 ý chính. Phần giải thích dài đưa vào lời thoại.
- Dùng nhiều sơ đồ luồng, bảng so sánh và ảnh chụp dashboard hơn là đoạn văn dài.
- Các từ khóa nên được nhấn mạnh: Kafka, Flink, CEP, Rules Engine, ONNX, ClickHouse, Grafana, APPROVE, ALERT, BLOCK.

## Cấu trúc tổng thể

```text
1-4:   Mở đầu, bài toán, mục tiêu
5-9:   Kiến trúc tổng thể và dữ liệu đầu vào
10-17: Xử lý realtime trong Flink và 3 lớp phát hiện
18-22: Decision, lưu trữ, dashboard, vận hành
23-28: Demo, điểm mạnh, hạn chế, cải tiến, kết luận
```

## Slide 1: Tiêu đề

Thông điệp chính: giới thiệu ngắn gọn hệ thống phát hiện gian lận giao dịch theo thời gian thực.

Nội dung trên slide:

- Real-Time Fraud Detection Platform
- Phát hiện gian lận giao dịch ngân hàng theo thời gian thực
- Kafka + Flink + Rules + CEP + ML + ClickHouse + Grafana
- Người trình bày, lớp/nhóm, ngày trình bày

Gợi ý visual:

- Nền tối, biểu tượng transaction stream, shield/security, dashboard.
- Một dòng flow nhỏ: Transaction -> Detection -> Decision -> Dashboard.

Lời thoại gợi ý:

```text
Em xin trình bày hệ thống Real-Time Fraud Detection Platform. Mục tiêu của hệ thống là mô phỏng một nền tảng phát hiện gian lận giao dịch ngân hàng theo thời gian thực, kết hợp rule, CEP và machine learning để đưa ra quyết định approve, alert hoặc block.
```

## Slide 2: Bối cảnh bài toán

Thông điệp chính: fraud detection cần xử lý nhanh, chính xác và giải thích được.

Nội dung trên slide:

- Giao dịch ngân hàng phát sinh liên tục với tốc độ cao.
- Gian lận cần được phát hiện trước hoặc ngay khi giao dịch hoàn tất.
- Dữ liệu có tính chuỗi: login, transfer, location, merchant, amount.
- Hệ thống cần vừa realtime, vừa có khả năng giải thích quyết định.

Gợi ý visual:

- Timeline giao dịch: normal transaction xen kẽ suspicious transaction.
- Highlight một transaction bị chặn.

Lời thoại gợi ý:

```text
Trong fraud detection, vấn đề không chỉ là phát hiện đúng mà còn phải phát hiện kịp thời. Nếu xử lý theo batch sau vài giờ thì giao dịch gian lận có thể đã hoàn tất. Vì vậy project tập trung vào streaming realtime và khả năng giải thích vì sao hệ thống cảnh báo hoặc chặn giao dịch.
```

## Slide 3: Mục tiêu hệ thống

Thông điệp chính: hệ thống cần xử lý stream, phát hiện đa lớp và trả quyết định cuối cùng.

Nội dung trên slide:

- Sinh giao dịch liên tục bằng Data Simulator.
- Đưa dữ liệu vào Kafka để làm event queue.
- Xử lý realtime bằng Apache Flink.
- Kết hợp 3 lớp phát hiện: Rules, CEP, ML.
- Ghi kết quả vào ClickHouse và hiển thị bằng Grafana.

Gợi ý visual:

- 5 mục tiêu dạng icon: generator, Kafka, Flink, detection, dashboard.

Lời thoại gợi ý:

```text
Hệ thống được xây dựng để mô phỏng end-to-end pipeline: từ sinh dữ liệu, đưa vào Kafka, xử lý bằng Flink, ra quyết định và quan sát kết quả trên Grafana. Điểm chính là không phụ thuộc vào một kỹ thuật duy nhất mà kết hợp nhiều lớp phát hiện.
```

## Slide 4: Kết quả đầu ra nghiệp vụ

Thông điệp chính: mọi giao dịch cuối cùng được đưa về 3 quyết định rõ ràng.

Nội dung trên slide:

- `APPROVE`: giao dịch bình thường.
- `ALERT`: giao dịch nghi ngờ, cần theo dõi hoặc điều tra.
- `BLOCK`: giao dịch rủi ro cao, cần chặn.
- Có `decision_source` để biết quyết định đến từ Rules, CEP hay ML.

Gợi ý visual:

- Bảng 3 cột: APPROVE màu xanh, ALERT màu cam, BLOCK màu đỏ.

Lời thoại gợi ý:

```text
Đầu ra nghiệp vụ được đơn giản hóa thành ba trạng thái. APPROVE cho giao dịch bình thường, ALERT cho giao dịch nghi ngờ, và BLOCK cho giao dịch rủi ro cao. Ngoài quyết định, hệ thống còn lưu nguồn quyết định để giải thích được lý do.
```

## Slide 5: Kiến trúc tổng thể

Thông điệp chính: pipeline gồm simulator, Kafka, Flink, ClickHouse, Grafana và MinIO.

Nội dung trên slide:

- Data Simulator tạo transaction và rule update.
- Kafka nhận stream đầu vào và output alert.
- Flink xử lý Rules, CEP, ML và Decision.
- ClickHouse lưu transaction và fraud alert.
- Grafana hiển thị dashboard realtime.
- MinIO lưu model ONNX và checkpoint/savepoint.

Gợi ý visual:

- Sơ đồ kiến trúc lớn:

```text
Data Simulator -> Kafka -> Flink -> ClickHouse -> Grafana
                         -> Kafka fraud-alerts
MinIO -> Flink model/checkpoint
```

Lời thoại gợi ý:

```text
Đây là kiến trúc tổng thể. Kafka đóng vai trò message broker, Flink là nơi xử lý chính, ClickHouse phục vụ lưu trữ phân tích, Grafana phục vụ quan sát, còn MinIO lưu model và state checkpoint.
```

## Slide 6: Vai trò từng công nghệ

Thông điệp chính: mỗi công nghệ được chọn theo đúng vai trò trong hệ realtime.

Nội dung trên slide:

| Thành phần | Vai trò |
|---|---|
| Kafka | Event streaming, buffer, replay |
| Flink | Stateful stream processing |
| CEP | Phát hiện chuỗi hành vi |
| ONNX Runtime | Chạy ML model trong JVM |
| ClickHouse | Lưu trữ phân tích tốc độ cao |
| Grafana | Dashboard realtime |
| MinIO | Object storage cho model và checkpoint |

Gợi ý visual:

- Bảng ngắn hoặc icon grid.

Lời thoại gợi ý:

```text
Kafka giúp tách producer và consumer, Flink xử lý realtime có state, CEP bắt các pattern theo thời gian, ONNX giúp chạy model trực tiếp trong Flink, ClickHouse tối ưu cho truy vấn dashboard, còn Grafana giúp quan sát vận hành.
```

## Slide 7: Kafka topics

Thông điệp chính: Kafka tách rõ input transaction, rule update và output alert.

Nội dung trên slide:

| Topic | Partitions | Ý nghĩa |
|---|---:|---|
| `transactions` | 8 | Giao dịch đầu vào |
| `fraud-rules` | 1 | Luật gian lận dạng JSON |
| `fraud-alerts` | 3 | Alert cuối cùng sau Decision Engine |

- Transaction dùng key `account_id`.
- Rule update ít nên dùng 1 partition.
- Alert topic giúp mở rộng sang SMS, email, case management.

Gợi ý visual:

- Sơ đồ ba topic Kafka với mũi tên vào/ra.

Lời thoại gợi ý:

```text
Topic transactions có 8 partitions vì đây là luồng tải cao. Simulator gửi key theo account_id để các event cùng tài khoản có xu hướng đi cùng partition. fraud-rules chỉ cần một partition vì rule update ít và cần thứ tự đơn giản.
```

## Slide 8: Data Simulator

Thông điệp chính: simulator tạo dữ liệu giả lập có hành vi, không chỉ random đơn giản.

Nội dung trên slide:

- 500 account, 200 merchant.
- Channel: ATM, ONLINE, POS, MOBILE.
- Event type: LOGIN, TRANSFER, PAYMENT, WITHDRAWAL, LOGIN_FAILED, LOGIN_SUCCESS.
- Location gồm Việt Nam và quốc tế.
- Cấu hình demo: khoảng 100 events/second, fraud ratio khoảng 1%.

Gợi ý visual:

- Minh họa generator tạo nhiều transaction vào Kafka.

Lời thoại gợi ý:

```text
Simulator tạo dữ liệu có cấu trúc gần với giao dịch ngân hàng: account, card, merchant, amount, channel, location và event type. Ngoài giao dịch bình thường, nó còn tạo các chuỗi bất thường để kiểm tra rule, CEP và ML.
```

## Slide 9: Schema giao dịch

Thông điệp chính: mỗi event chứa đủ thông tin để xử lý theo account, thời gian, vị trí và số tiền.

Nội dung trên slide:

Các field chính:

- `transaction_id`, `account_id`, `card_id`
- `timestamp`, `produced_at`
- `amount`, `event_type`, `channel`
- `location`, `merchant_id`, `status`
- `is_fraud` từ simulator

Lưu ý:

- `timestamp` dùng cho event time trong Flink.
- `produced_at` dùng tính end-to-end latency.

Gợi ý visual:

- JSON card rút gọn, highlight timestamp, amount, location, account_id.

Lời thoại gợi ý:

```text
Hai trường thời gian quan trọng là timestamp và produced_at. timestamp là thời điểm nghiệp vụ của event nên dùng cho event time. produced_at là thời điểm simulator gửi event, dùng để tính latency từ lúc phát sinh đến lúc hệ thống ra quyết định.
```

## Slide 10: Các kiểu gian lận giả lập

Thông điệp chính: dữ liệu test bao phủ nhiều dạng gian lận khác nhau.

Nội dung trên slide:

| Kiểu fraud | Mô tả ngắn |
|---|---|
| Account takeover | Failed login -> success -> transfer |
| Rapid transactions | Nhiều giao dịch liên tiếp rất nhanh, bắt bằng CEP velocity |
| Impossible travel | Hai vị trí xa nhau trong thời gian ngắn |
| High amount | Giao dịch số tiền lớn |
| Micro transactions | Nhiều giao dịch nhỏ liên tục |

Gợi ý visual:

- 5 card nhỏ, mỗi card có icon và ví dụ pattern.

Lời thoại gợi ý:

```text
Simulator không chỉ tạo một loại fraud. Nó tạo nhiều tình huống như account takeover, giao dịch liên tiếp, impossible travel, giao dịch số tiền lớn và nhiều giao dịch nhỏ. Nhờ vậy có thể kiểm thử nhiều lớp phát hiện khác nhau.
```

## Slide 11: Flink job tổng quan

Thông điệp chính: Flink là trung tâm xử lý realtime của toàn hệ thống.

Nội dung trên slide:

```text
KafkaSource<Transaction>
  -> Filter invalid transactions
  -> Rules Engine
  -> CEP Pattern Detector
  -> CepAlertMerger
  -> FeatureAggregator
  -> ONNX ML Scorer
  -> DecisionMaker
  -> ClickHouse + Kafka fraud-alerts
```

- Parallelism: 8.
- Checkpoint: mỗi 60 giây.
- State lưu theo account.

Gợi ý visual:

- Pipeline ngang, mỗi operator là một block.

Lời thoại gợi ý:

```text
Luồng chính trong Flink bắt đầu từ Kafka source, sau đó transaction đi qua rule engine, CEP, feature aggregation, ML scoring và decision maker. Cuối cùng kết quả được ghi vào ClickHouse và fraud-alerts topic.
```

## Slide 12: Event time và watermark

Thông điệp chính: hệ thống xử lý theo thời gian sự kiện, không chỉ thời gian server nhận event.

Nội dung trên slide:

- Flink dùng `timestamp` của transaction làm event time.
- Watermark cho phép out-of-order tối đa 5 giây.
- Idleness 30 giây tránh watermark bị kẹt khi partition không có dữ liệu.
- CEP cần event time để hiểu "trong 2 phút", "trong 10 phút", "trong 15 phút".

Gợi ý visual:

- Timeline event đến không đúng thứ tự, watermark chạy phía sau.

Lời thoại gợi ý:

```text
Trong streaming, event có thể đến muộn hoặc không đúng thứ tự. Nếu chỉ dùng processing time, CEP có thể đánh sai chuỗi hành vi. Vì vậy hệ thống dùng event time và watermark 5 giây để cân bằng giữa độ chính xác và độ trễ.
```

## Slide 13: Lớp phát hiện 1 - Dynamic Rules Engine

Thông điệp chính: rule bắt các trường hợp rõ ràng và có thể cập nhật runtime.

Nội dung trên slide:

- Rule được gửi vào Kafka topic `fraud-rules`.
- Flink dùng Broadcast State để mọi subtask nhận cùng bộ rule.
- Có thể cập nhật rule mà không restart job.
- Rule hỗ trợ operator: GREATER_THAN, LESS_THAN, EQUALS, IN, NOT_IN.

Ví dụ rule:

- Amount > 10000 -> HIGH.
- Merchant trong blacklist -> CRITICAL.
- Large transfer ban đêm -> MEDIUM.

Gợi ý visual:

- Sơ đồ fraud-rules topic broadcast tới nhiều Flink subtasks.

Lời thoại gợi ý:

```text
Rules Engine phù hợp cho các điều kiện rõ ràng, ví dụ merchant nằm trong blacklist hoặc amount vượt ngưỡng. Điểm quan trọng là rule không hard-code cố định trong job mà đi qua Kafka và được broadcast đến các subtasks.
```

## Slide 14: Vì sao dùng Broadcast State?

Thông điệp chính: Broadcast State giúp rule nhất quán trên toàn bộ parallel tasks.

Nội dung trên slide:

- Rule là dữ liệu nhỏ nhưng cần có ở mọi task.
- Transaction volume cao, không nên query database cho từng event.
- Rule update ít, transaction nhiều.
- Cache rule đã parse để tránh parse JSON lặp lại.

Gợi ý visual:

```text
fraud-rules -> broadcast -> task 1
                         -> task 2
                         -> task 3
                         -> task 8
```

Lời thoại gợi ý:

```text
Nếu Flink chạy parallelism 8 thì mỗi subtask xử lý một phần transaction. Tất cả subtasks đều cần cùng một bộ rule mới nhất. Broadcast State giải quyết bài toán này, đồng thời giảm chi phí gọi database ngoài.
```

## Slide 15: Lớp phát hiện 2 - CEP Engine

Thông điệp chính: CEP phát hiện chuỗi hành vi, không chỉ một transaction đơn lẻ.

Nội dung trên slide:

- CEP = Complex Event Processing.
- Input được `keyBy(accountId)`.
- Pattern xét riêng theo từng tài khoản.
- Detect 3 pattern chính:
  - Account Takeover
  - Micro Transactions
  - Impossible Travel

Gợi ý visual:

- Timeline theo một account, các event nối thành pattern.

Lời thoại gợi ý:

```text
Nhiều gian lận không thể nhìn ra từ một event đơn lẻ. Ví dụ account takeover cần chuỗi failed login, login success, rồi transfer. CEP được dùng để bắt các chuỗi sự kiện theo đúng thứ tự và trong time window.
```

## Slide 16: CEP Pattern chi tiết

Thông điệp chính: mỗi pattern có điều kiện, cửa sổ thời gian và mức độ rủi ro riêng.

Nội dung trên slide:

| Pattern | Điều kiện | Window | Severity |
|---|---|---:|---|
| Account Takeover | >= 3 failed login -> success -> transfer | 10 phút | HIGH/CRITICAL |
| Micro Transactions | >= 5 giao dịch < 10 | 2 phút | MEDIUM/HIGH/CRITICAL |
| Velocity Attack | >= 3 transfer/payment 100-2000 rất nhanh | 2 giây | HIGH/CRITICAL |
| Impossible Travel | 2 location cách >= 150km | 15 phút | HIGH/CRITICAL |

Gợi ý visual:

- Bảng ngắn kết hợp mini timeline cho từng pattern.

Lời thoại gợi ý:

```text
Bốn pattern được thiết kế cho các nhóm rủi ro khác nhau. Account takeover nhìn vào login sequence, micro transactions nhìn vào nhiều giao dịch nhỏ, velocity attack bắt chuỗi giao dịch trung bình xảy ra quá nhanh, còn impossible travel tính khoảng cách Haversine giữa hai tọa độ trong thời gian ngắn.
```

## Slide 17: CepAlertMerger

Thông điệp chính: merger đảm bảo alert CEP được gắn lại đúng transaction trước khi ra quyết định.

Nội dung trên slide:

- Main stream đi rất nhanh.
- CEP branch cần thời gian để match pattern.
- Nếu không merge cẩn thận, transaction có thể bị approve trước khi CEP alert tới.
- Giải pháp: buffer transaction khoảng 10ms.
- Gắn alert CEP vào transaction trước DecisionMaker.

Gợi ý visual:

```text
Transaction stream ---------> buffer 10ms -> Decision
        \                         ^
         -> CEP branch -> alert --|
```

Lời thoại gợi ý:

```text
CEP chạy ở một nhánh riêng nên alert có thể đến sau transaction gốc. CepAlertMerger buffer transaction trong thời gian rất ngắn để có cơ hội gắn alert CEP vào đúng transaction trước khi DecisionMaker xử lý.
```

## Slide 18: Feature Aggregator

Thông điệp chính: ML cần feature hành vi theo thời gian, không chỉ dữ liệu của event hiện tại.

Nội dung trên slide:

- Dùng Keyed State theo `account_id`.
- Tính thống kê hành vi gần đây.
- Feature ví dụ:
  - transaction frequency trong 1 giờ
  - tổng amount trong 1 giờ
  - average amount
  - số merchant khác nhau
  - thay đổi vị trí
- State được checkpoint để fault tolerance.

Gợi ý visual:

- Một account timeline được gom thành feature vector.

Lời thoại gợi ý:

```text
ML model không chỉ nhìn amount của một transaction. FeatureAggregator gom lịch sử gần đây theo account để tạo ra các đặc trưng như tần suất giao dịch, tổng tiền, merchant đa dạng và thay đổi vị trí.
```

## Slide 19: Lớp phát hiện 3 - ML Scoring với ONNX

Thông điệp chính: model Random Forest được export ONNX và chạy trực tiếp trong Flink.

Nội dung trên slide:

- `ml-trainer` train Random Forest.
- Model được export sang ONNX.
- Upload model vào MinIO.
- Flink tải model và chạy ONNX Runtime trong JVM.
- Không cần model serving service riêng.

Trade-off:

- Latency thấp hơn vì không gọi network.
- Muốn update model cần reload/restart hoặc bổ sung cơ chế versioning.

Gợi ý visual:

- Training -> ONNX file -> MinIO -> Flink scorer.

Lời thoại gợi ý:

```text
Model Random Forest được train bên ngoài, export sang ONNX rồi lưu lên MinIO. Khi Flink job chạy, OnnxModelScorer tải model và inference trực tiếp trong JVM. Cách này giảm network round trip so với gọi một service ML riêng.
```

## Slide 20: Decision Engine

Thông điệp chính: DecisionMaker hợp nhất Rules, CEP và ML thành quyết định cuối cùng.

Nội dung trên slide:

- Input: rule alerts, CEP alerts, ML score.
- Output: `APPROVE`, `ALERT`, `BLOCK`.
- Ưu tiên xử lý:
  - CRITICAL rule/CEP -> BLOCK.
  - ML score rất cao -> BLOCK.
  - Rule/CEP/ML nghi ngờ -> ALERT.
  - Không có tín hiệu rủi ro -> APPROVE.
- Lưu `decision_source` để giải thích.

Gợi ý visual:

- Decision matrix hoặc flowchart.

Lời thoại gợi ý:

```text
DecisionMaker là nơi tổng hợp các tín hiệu. Nếu có rule hoặc CEP severity critical thì ưu tiên block. Nếu ML score cao cũng có thể block. Các tín hiệu trung bình đưa về alert, còn giao dịch không có dấu hiệu rủi ro thì approve.
```

## Slide 21: Ví dụ logic quyết định

Thông điệp chính: hệ thống giải thích được vì sao giao dịch bị alert hoặc block.

Nội dung trên slide:

| Tình huống | Kết quả |
|---|---|
| Merchant = `MERCH-0199` trong blacklist | BLOCK, source = RULES_ENGINE |
| 5 giao dịch nhỏ trong 2 phút, nhiều merchant | BLOCK/ALERT, source = CEP_ENGINE |
| Amount = 15,000, ML score = 0.4 | ALERT, source = RULES_ENGINE |
| Amount bình thường, ML score = 0.91 | BLOCK, source = ML_SCORING |
| Không rule, không CEP, ML thấp | APPROVE |

Gợi ý visual:

- Bảng 5 dòng với màu xanh/cam/đỏ theo quyết định.

Lời thoại gợi ý:

```text
Đây là các ví dụ dễ dùng khi bảo vệ. Hệ thống không chỉ trả về kết quả mà còn chỉ ra nguồn quyết định, ví dụ do blacklist rule, do CEP micro transaction hoặc do ML score cao.
```

## Slide 22: ClickHouse storage

Thông điệp chính: ClickHouse lưu kết quả để dashboard query nhanh.

Nội dung trên slide:

- Bảng `transactions`: lưu quyết định cuối của từng giao dịch.
- Bảng `fraud_alerts`: lưu alert cuối.
- Dùng ReplacingMergeTree để hỗ trợ dedup.
- Có TTL cho dữ liệu.
- Có materialized views cho aggregate: throughput, alert by type, geo, latency.

Gợi ý visual:

- Sơ đồ Flink sink -> ClickHouse tables -> materialized views.

Lời thoại gợi ý:

```text
ClickHouse được chọn vì dashboard cần count, group by và aggregate trên nhiều record theo thời gian. Đây là workload phù hợp với columnar database. Các bảng cũng được thiết kế để hỗ trợ dedup gần idempotent.
```

## Slide 23: Exactly-once và sink thực tế

Thông điệp chính: Flink source/state exactly-once, ClickHouse đạt near exactly-once bằng idempotent/dedup.

Nội dung trên slide:

- Flink checkpoint mỗi 60 giây.
- Kafka offset commit theo checkpoint.
- State được checkpoint vào MinIO.
- ClickHouse custom sink không phải TwoPhaseCommit sink.
- Dùng retry + checkpoint buffer + ReplacingMergeTree để đạt near exactly-once.

Gợi ý visual:

- Bảng phân biệt:

| Phần | Mức đảm bảo |
|---|---|
| Kafka source + Flink state | Exactly-once |
| ClickHouse custom sink | Near exactly-once |

Lời thoại gợi ý:

```text
Điểm này cần trình bày trung thực. Flink source và state đạt exactly-once theo checkpoint. Nhưng sink ClickHouse custom không phải two-phase commit, nên end-to-end tới ClickHouse nên gọi là near exactly-once, dựa trên retry và dedup theo transaction_id.
```

## Slide 24: Grafana dashboard

Thông điệp chính: Grafana giúp quan sát realtime throughput, latency và phân bố quyết định.

Nội dung trên slide:

- Total Transactions.
- Fraud Detected: số giao dịch hệ thống đánh fraud.
- Ground Truth Fraud Rate: tỷ lệ fraud thật trong dữ liệu simulator.
- FDR/Recall: tỷ lệ fraud thật được hệ thống bắt đúng.
- FPR: tỷ lệ giao dịch bình thường bị báo nhầm.
- Precision: trong các alert/block, bao nhiêu phần trăm thật sự là fraud.
- BLOCKED Transactions.
- Avg ML Score.
- Total Alerts.
- Throughput events/second.
- End-to-end latency avg, p95, p99.
- Breakdown theo decision, source, alert type.

Gợi ý visual:

- Chèn ảnh chụp dashboard Grafana nếu có.
- Nếu chưa có ảnh, dùng wireframe dashboard 6 panels.

Lời thoại gợi ý:

```text
Dashboard phục vụ quan sát vận hành, không nằm trong đường quyết định giao dịch. Ngoài throughput và latency, dashboard hiện tách rõ detected fraud với ground truth. FDR cao cho thấy hệ thống ít bỏ sót fraud, còn FPR và Precision cho biết mức độ báo nhầm, tức ảnh hưởng tới trải nghiệm người dùng và vận hành.
```

## Slide 25: Demo flow

Thông điệp chính: demo cho thấy hệ thống chạy end-to-end từ container tới dashboard.

Nội dung trên slide:

Các bước demo:

1. Chạy `docker compose up --build -d`.
2. Kiểm tra Kafka, Flink, ClickHouse, Grafana.
3. Mở Flink UI: `http://localhost:8081`.
4. Mở Grafana: `http://localhost:3000`.
5. Quan sát transaction count, alert count, latency, decision breakdown.
6. Query nhanh ClickHouse để kiểm tra dữ liệu.

Gợi ý visual:

- Checklist demo hoặc ảnh 4 màn hình: Docker, Flink UI, Grafana, ClickHouse query.

Lời thoại gợi ý:

```text
Trong phần demo, em sẽ chạy toàn bộ stack bằng Docker Compose. Sau khi các service sẵn sàng, simulator gửi giao dịch vào Kafka, Flink job xử lý và Grafana bắt đầu hiển thị transaction, alert và latency.
```

## Slide 26: Điểm mạnh của hệ thống

Thông điệp chính: hệ thống có kiến trúc realtime, đa lớp, giải thích được và có khả năng mở rộng.

Nội dung trên slide:

- Multi-layer detection: Rules + CEP + ML.
- Explainability qua `decision_source`, `rule_triggered`, `ml_score`.
- Rule update động qua Kafka Broadcast State.
- Stateful realtime theo account.
- Checkpoint và MinIO cho fault tolerance.
- Dashboard realtime bằng Grafana.
- Có thể mở rộng bằng tăng Kafka partitions và Flink parallelism.

Gợi ý visual:

- 7 điểm mạnh dạng icon grid.

Lời thoại gợi ý:

```text
Điểm mạnh lớn nhất là hệ thống không phụ thuộc hoàn toàn vào ML. Rule giúp giải thích tốt, CEP bắt được chuỗi hành vi, ML hỗ trợ phát hiện pattern xác suất. Ngoài ra hệ thống có state, checkpoint, dashboard và khả năng mở rộng.
```

## Slide 27: Hạn chế và cách trả lời phản biện

Thông điệp chính: nắm rõ giới hạn hiện tại và hướng xử lý nếu đưa vào production.

Nội dung trên slide:

| Hạn chế | Cách xử lý |
|---|---|
| Dữ liệu synthetic | Thay bằng stream từ core banking/card system |
| Ground truth hiện là nhãn giả lập | Validate thêm bằng dữ liệu thật hoặc public dataset |
| FPR còn cao, Precision thấp | Tune threshold, phân tầng ALERT/BLOCK, calibration |
| ClickHouse sink near exactly-once | Dùng transactional/idempotent sink chặt hơn |
| Single Kafka broker | Production dùng cluster 3+ brokers |
| State backend đang là HashMap | Chuyển RocksDB khi state lớn |
| Model chưa reload động | Thêm model registry/versioning |

Gợi ý visual:

- Bảng hai cột: limitation và improvement.

Lời thoại gợi ý:

```text
Project hiện là demo kiến trúc nên có một số giới hạn. Hệ thống đã có ground truth từ simulator để tính FDR/FPR, nhưng ground truth này vẫn là synthetic. Metric hiện tại cho thấy FDR rất cao, tức ít bỏ sót fraud, nhưng FPR còn cao nên nếu production cần tune threshold, giảm cảnh báo nhầm và kiểm định bằng dữ liệu thật.
```

## Slide 28: Kết luận và hướng phát triển

Thông điệp chính: hệ thống chứng minh được pipeline realtime fraud detection end-to-end và còn có hướng nâng cấp rõ ràng.

Nội dung trên slide:

- Đã xây dựng pipeline realtime từ data generation tới dashboard.
- Flink xử lý stateful stream với watermark, checkpoint và CEP.
- Kết hợp Rules, CEP, ML để tăng khả năng phát hiện và giải thích.
- ClickHouse và Grafana hỗ trợ quan sát vận hành.
- Hướng phát triển:
  - giảm false positive và tăng precision
  - thêm confusion matrix, Precision/Recall/F1 và case-level metrics
  - model versioning/reload
  - production Kafka/ClickHouse cluster
  - security và monitoring đầy đủ hơn

Gợi ý visual:

- Sơ đồ roadmap ngắn: Demo -> Evaluation -> Production hardening.

Lời thoại gợi ý:

```text
Tóm lại, hệ thống đã chứng minh được một kiến trúc realtime fraud detection end-to-end. Điểm cốt lõi là xử lý stream bằng Flink, kết hợp nhiều lớp phát hiện và có dashboard quan sát. Hệ thống hiện có recall cao nhưng false positive còn lớn, nên nếu phát triển tiếp em sẽ ưu tiên tuning threshold, tăng precision, bổ sung model governance và hardening cho production.
```

## Slide phụ nếu muốn thay thế

Nếu cần rút còn 25 slide:

- Gộp slide 13 và 14 thành một slide về Rules Engine.
- Gộp slide 17 và 18 thành một slide về CEP merger + feature aggregation.
- Gộp slide 22 và 23 thành một slide về storage và delivery guarantee.

Nếu muốn tăng lên 30 slide:

- Tách riêng slide về `fraud-rules` JSON examples.
- Tách riêng slide về công thức latency và FDR/FPR.

## Kịch bản nói 1 phút cuối

```text
Hệ thống của em là nền tảng phát hiện gian lận giao dịch theo thời gian thực.
Data Simulator sinh giao dịch và rule update vào Kafka.
Flink đọc stream giao dịch, xử lý theo event time với watermark 5 giây, sau đó chạy qua 3 lớp phát hiện: Rules Engine dùng Broadcast State để cập nhật luật động, CEP Engine bắt các chuỗi hành vi như account takeover, micro transactions, velocity attack và impossible travel, ML Scoring dùng model Random Forest export ONNX và chạy trực tiếp trong JVM.
Decision Engine gom kết quả theo priority, xuất APPROVE, ALERT hoặc BLOCK.
Kết quả được ghi vào ClickHouse bằng batch async sink và hiển thị trên Grafana.
Hệ thống có checkpoint, state theo account, dashboard latency/throughput và có thể mở rộng alert sang SMS, email, case management hoặc core banking.
```
