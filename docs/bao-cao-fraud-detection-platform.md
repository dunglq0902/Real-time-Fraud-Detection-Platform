# REAL-TIME FRAUD DETECTION PLATFORM

[Họ và tên sinh viên:Lưu Quốc Dũng], [Họ và tên mentor:Phạm Việt Hoà]1  
1 [Đơn vị mentor hướng dẫn: VTS] - [email mentor hướng dẫnv:hoapv30@viettel.com.vn]

## 1. GIỚI THIỆU CHUNG

Trong các hệ thống ngân hàng, ví điện tử và thanh toán số, giao dịch phát sinh liên tục với tần suất lớn. Nếu gian lận chỉ được phát hiện theo lô sau vài giờ hoặc cuối ngày, tổn thất có thể đã xảy ra và việc xử lý sau đó trở nên khó khăn. Vì vậy, nhu cầu thực tiễn đặt ra là xây dựng một hệ thống có khả năng tiếp nhận giao dịch theo thời gian thực, đánh giá rủi ro ngay khi sự kiện xuất hiện, đồng thời cung cấp lý do rõ ràng cho từng quyết định nghiệp vụ.

Đề tài xây dựng nền tảng Real-Time Fraud Detection Platform nhằm mô phỏng một pipeline phát hiện gian lận giao dịch end-to-end. Hệ thống nhận dữ liệu từ Data Simulator, đưa vào Apache Kafka, xử lý bằng Apache Flink, kết hợp ba lớp phát hiện gồm Rules Engine, Complex Event Processing (CEP) và Machine Learning, sau đó lưu kết quả vào ClickHouse và hiển thị trên Grafana. Đầu ra nghiệp vụ được chuẩn hóa thành ba quyết định: `APPROVE`, `ALERT` và `BLOCK`.

Mục tiêu chính của đề tài là:

- Xây dựng pipeline realtime có khả năng xử lý luồng giao dịch liên tục.
- Phát hiện nhiều dạng gian lận như giao dịch giá trị lớn, merchant nằm trong blacklist, account takeover, velocity attack, micro transactions và impossible travel.
- Kết hợp luật nghiệp vụ, chuỗi hành vi theo thời gian và điểm rủi ro từ mô hình ML để nâng cao hiệu quả phát hiện.
- Lưu trữ và trực quan hóa kết quả để đánh giá throughput, latency, tỷ lệ phát hiện gian lận và false positive.
- Thiết kế hệ thống có khả năng giải thích quyết định thông qua các trường như `decision_source`, `rule_triggered`, `ml_score` và `combined_score`.

Sinh viên trực tiếp tham gia nghiên cứu bài toán, thiết kế kiến trúc, triển khai Data Simulator, Flink job, Rules Engine, CEP patterns, Feature Aggregator, ONNX ML Scoring, Decision Engine, ClickHouse schema, dashboard Grafana, Docker Compose và tài liệu kỹ thuật. Đóng góp chính nằm ở việc chuyển bài toán phát hiện gian lận từ một mô hình dự đoán đơn lẻ thành một nền tảng realtime nhiều lớp, có state, có khả năng quan sát và có thể mở rộng.

## 2. NỘI DUNG VÀ PHƯƠNG PHÁP

Quy trình triển khai được thiết kế theo luồng dữ liệu sau:

```text
Data Simulator -> Kafka -> Apache Flink -> ClickHouse -> Grafana
                              |
                              +-> Kafka fraud-alerts
```

Data Simulator được viết bằng Python để sinh dữ liệu giao dịch giả lập với khoảng 500 tài khoản, 200 merchant, nhiều kênh giao dịch như `ATM`, `ONLINE`, `POS`, `MOBILE`, cùng các loại sự kiện như `TRANSFER`, `PAYMENT`, `WITHDRAWAL`, `LOGIN_FAILED`, `LOGIN_SUCCESS`. Dữ liệu có cả vị trí trong nước và quốc tế để kiểm thử các hành vi bất thường theo địa lý. Simulator cũng gửi các rule ban đầu vào topic `fraud-rules`, ví dụ `amount > 10000`, merchant blacklist và giao dịch lớn vào ban đêm.

Kafka đóng vai trò ingestion layer. Hệ thống sử dụng các topic chính gồm `transactions` với 8 partitions, `fraud-rules` với 1 partition và `fraud-alerts` với 3 partitions. Transaction được gửi theo key `account_id` để hỗ trợ xử lý state theo tài khoản trong Flink.

Apache Flink là lõi xử lý realtime, được triển khai bằng Java với parallelism 8, checkpoint mỗi 60 giây, event time, watermark trễ tối đa 5 giây và state theo account. Pipeline Flink gồm các bước chính:

- Deserialize và kiểm tra transaction hợp lệ.
- Áp dụng Rules Engine bằng Broadcast State để cập nhật luật động qua Kafka mà không cần restart job.
- Phát hiện chuỗi hành vi bằng Flink CEP.
- Gộp alert từ nhánh CEP về transaction gốc bằng `CepAlertMerger`.
- Tính feature hành vi theo account bằng `FeatureAggregator`.
- Chấm điểm rủi ro bằng mô hình Random Forest export sang ONNX và chạy trực tiếp trong JVM bằng ONNX Runtime.
- Tổng hợp tín hiệu bằng `DecisionMaker` để đưa ra `APPROVE`, `ALERT` hoặc `BLOCK`.

Các pattern CEP được xây dựng theo hướng mô phỏng nghiệp vụ thực tế:

- Account takeover: nhiều lần `LOGIN_FAILED`, sau đó `LOGIN_SUCCESS` và `TRANSFER` trong 10 phút.
- Micro transactions: nhiều giao dịch nhỏ dưới 10 đơn vị tiền trong 2 phút.
- Velocity attack: nhiều giao dịch trung bình trong khoảng 2 giây.
- Impossible travel: hai giao dịch tài chính cách nhau trên 150 km trong vòng 15 phút, tính bằng công thức Haversine.

Lớp Machine Learning sử dụng dữ liệu synthetic 50.000 mẫu để huấn luyện Random Forest với các feature: `amount`, `hour_of_day`, `is_foreign`, `tx_frequency_1h`, `distance_km`. Mô hình được export sang ONNX, upload lên MinIO và được Flink tải khi job khởi động. Cách tiếp cận này giúp inference diễn ra in-process, giảm độ trễ so với việc gọi một model-serving service bên ngoài.

ClickHouse được dùng làm kho phân tích do phù hợp với truy vấn dạng time-series, aggregate và dashboard. Hệ thống có bảng `transactions`, `fraud_alerts`, TTL dữ liệu, `ReplacingMergeTree` để hỗ trợ dedup và các materialized view cho throughput, latency, alert by type và fraud by geography. Grafana đọc dữ liệu từ ClickHouse để hiển thị tổng số giao dịch, số alert, số block, throughput, latency, FDR/FPR, decision breakdown và recent alerts.

Ý tưởng sáng tạo của sinh viên là thiết kế cơ chế phát hiện nhiều lớp thay vì chỉ dùng rule hoặc ML đơn lẻ. Rules giúp phát hiện điều kiện rõ ràng và dễ giải thích; CEP phát hiện chuỗi hành vi mà một transaction riêng lẻ không thể biểu hiện đầy đủ; ML bổ sung khả năng đánh giá rủi ro dựa trên tổ hợp feature. Ngoài ra, việc cập nhật rule động bằng Broadcast State, gộp alert CEP tránh race condition và lưu `decision_source` giúp hệ thống vừa linh hoạt vừa có khả năng giải thích.

## 3. KẾT QUẢ THỰC HIỆN

Đề tài đã hoàn thành một hệ thống demo end-to-end có thể chạy bằng Docker Compose. Các thành phần chính bao gồm:

- `data-simulator`: sinh transaction và rule update theo thời gian thực.
- `flink-job`: xử lý stream bằng Kafka Source, Rules Engine, CEP Engine, Feature Aggregator, ONNX Scorer, Decision Maker và các sink.
- `ml-model`: huấn luyện Random Forest, export model ONNX và lưu vào MinIO.
- `clickhouse`: khởi tạo database, bảng lưu giao dịch, bảng alert và materialized views.
- `grafana`: dashboard giám sát realtime.
- `docker-compose.yml`: bộ cài/chạy toàn bộ stack gồm Kafka, MinIO, ClickHouse, Flink, ML trainer, simulator và Grafana.

Các tính năng nổi bật đã đạt được:

- Xử lý luồng giao dịch realtime với Kafka và Flink.
- Phát hiện gian lận theo luật động qua topic `fraud-rules`.
- Phát hiện chuỗi hành vi phức tạp bằng Flink CEP.
- Tính feature hành vi theo tài khoản trong cửa sổ 1 giờ và 24 giờ.
- Chạy ML inference bằng ONNX Runtime trong JVM.
- Ra quyết định nghiệp vụ `APPROVE`, `ALERT`, `BLOCK`.
- Lưu kết quả vào ClickHouse và phát alert ra Kafka topic `fraud-alerts`.
- Dashboard Grafana cho throughput, latency, fraud rate, FDR/FPR, decision source và recent fraud alerts.
- Có unit test cho các logic nền tảng như Haversine, deserialization, rule model và DecisionMaker.

Một số minh chứng kỹ thuật cụ thể:

| Hạng mục | Kết quả triển khai |
|---|---|
| Kafka topics | `transactions` 8 partitions, `fraud-rules` 1 partition, `fraud-alerts` 3 partitions |
| Flink processing | Parallelism 8, checkpoint 60 giây, event-time watermark 5 giây |
| CEP patterns | Account takeover, micro transactions, velocity attack, impossible travel |
| ML model | Random Forest, 100 trees, max depth 10, export ONNX |
| Feature ML | `amount`, `hour_of_day`, `is_foreign`, `tx_frequency_1h`, `distance_km` |
| Storage | ClickHouse `ReplacingMergeTree`, TTL 90 ngày cho transactions, 180 ngày cho alerts |
| Dashboard | Grafana + ClickHouse datasource, theo dõi realtime metric và bảng alert |

Mã nguồn được tổ chức theo từng module rõ ràng trong repository. Bộ cài chương trình là `docker-compose.yml`, cho phép build và chạy toàn bộ hệ thống bằng lệnh:

```bash
docker compose up --build -d
```

Các giao diện kiểm tra chính gồm Grafana tại `http://localhost:3000`, Flink Web UI tại `http://localhost:8081`, MinIO Console tại `http://localhost:9001` và ClickHouse HTTP tại `http://localhost:8123`.

## 4. ĐÁNH GIÁ HIỆU QUẢ

Hệ thống được đánh giá theo bốn nhóm tiêu chí: hiệu quả xử lý realtime, hiệu quả phát hiện gian lận, khả năng giải thích và khả năng vận hành.

Về xử lý realtime, kiến trúc Kafka - Flink cho phép tách producer và consumer, xử lý song song theo partition và theo account state. Mục tiêu cấu hình của hệ thống là throughput trên 1.000 events/giây, mức tối ưu trên 5.000 events/giây, latency end-to-end dưới 500 ms và mức chấp nhận dưới 2 giây. Trong demo hiện tại, simulator được cấu hình tạo khoảng 500 events/giây, phù hợp để kiểm thử luồng dữ liệu, dashboard và quyết định realtime trên máy cá nhân.

Qua quan sát trên Flink Web UI, điểm nóng hiệu năng hiện tại nằm ở nhánh CEP - Impossible Travel. Operator này có mức Busy ổn định khoảng 40%, trong khi các nhánh xử lý khác gần như 0% và Backpressure của toàn job vẫn ở mức 0%. Điều này cho thấy pipeline chưa bị nghẽn ở downstream, nhưng Impossible Travel là phần tốn tài nguyên nhất vì phải giữ state theo `account_id`, so sánh các giao dịch trong cửa sổ 15 phút, kiểm tra tọa độ hợp lệ và tính khoảng cách Haversine giữa hai vị trí. Nếu tăng tốc độ sinh dữ liệu hoặc mở rộng số lượng account, nhánh này có khả năng trở thành bottleneck đầu tiên của hệ thống. Hướng xử lý là tối ưu riêng CEP4 bằng cách lọc sớm chỉ các giao dịch tài chính có tọa độ hợp lệ, giới hạn TTL/state và window theo nhu cầu nghiệp vụ, kiểm soát duplicate/overlap match bằng skip strategy, chuyển sang RocksDB state backend khi state lớn, đồng thời tăng Kafka partitions và Flink parallelism cho nhánh CEP khi triển khai ở tải cao hơn.

Về phát hiện gian lận, hệ thống không phụ thuộc vào một phương pháp duy nhất. So với cách chỉ dùng rule cố định, CEP giúp phát hiện hành vi theo chuỗi như account takeover hoặc impossible travel. So với cách chỉ dùng ML, Rules và CEP giúp tăng khả năng giải thích và tránh bỏ sót các điều kiện nghiệp vụ rõ ràng như merchant blacklist. Decision Engine tổng hợp nhiều tín hiệu để giảm rủi ro ra quyết định dựa trên một nguồn duy nhất.

Về khả năng giải thích, mỗi transaction sau xử lý đều có thông tin quyết định, nguồn quyết định, rule bị kích hoạt, điểm ML và điểm tổng hợp. Đây là điểm quan trọng trong nghiệp vụ tài chính vì người vận hành cần biết vì sao giao dịch bị cảnh báo hoặc bị chặn. Việc tách `ground_truth_is_fraud` từ simulator và `is_fraud` do hệ thống phát hiện cũng giúp dashboard tính các chỉ số FDR/FPR đúng bản chất hơn.

Về khả năng vận hành, hệ thống có Docker Compose để triển khai nhanh, dashboard theo dõi realtime, checkpoint Flink và lưu checkpoint/savepoint vào MinIO. ClickHouse sink hiện đạt mức near exactly-once nhờ checkpoint buffer và dedup bằng `ReplacingMergeTree`; tuy nhiên chưa phải exactly-once tuyệt đối tới ClickHouse vì custom sink chưa dùng Two Phase Commit. Đây là đánh giá trung thực, đồng thời cho thấy hướng cải tiến rõ ràng nếu triển khai production.

So với phương pháp xử lý batch truyền thống, giải pháp này có ưu điểm là phát hiện và phản ứng gần thời gian thực, theo dõi được latency/throughput, cập nhật rule động và dễ mở rộng sang notification, case management hoặc core banking thông qua topic `fraud-alerts`. Hạn chế hiện tại là dữ liệu và model còn synthetic, Kafka demo chỉ có một broker, state backend trong cấu hình local còn dùng HashMap, hệ thống chưa có schema registry, model versioning và monitoring hạ tầng đầy đủ.

## 5. KẾT LUẬN

Đề tài đã xây dựng được một nền tảng phát hiện gian lận giao dịch theo thời gian thực từ khâu sinh dữ liệu, ingestion, xử lý stream, chấm điểm rủi ro, ra quyết định, lưu trữ đến trực quan hóa. Sản phẩm thể hiện được tính mới ở cách kết hợp Rules Engine, CEP Engine và ML Scoring trong cùng một pipeline Flink có state. Tính sáng tạo thể hiện ở rule update động, phát hiện chuỗi hành vi theo event time, inference ONNX trực tiếp trong JVM và thiết kế output có khả năng giải thích.

Về hiệu quả, hệ thống đáp ứng mục tiêu demo một kiến trúc realtime hoàn chỉnh, có dashboard giám sát, có ground truth để đánh giá, có checkpoint và có khả năng mở rộng theo partition/parallelism. Đóng góp của sinh viên không chỉ nằm ở việc ghép các công nghệ, mà còn ở việc thiết kế logic nghiệp vụ, xử lý trạng thái, xây dựng luồng quyết định và nhận diện các giới hạn kỹ thuật một cách rõ ràng.

Trong tương lai, đề tài có thể phát triển theo các hướng: đánh giá trên dữ liệu fraud thực tế hoặc public dataset, giảm false positive bằng tuning threshold và calibration model, bổ sung confusion matrix/Precision/Recall/F1 theo thời gian, thêm model versioning và reload động, dùng Kafka cluster nhiều broker, chuyển sang RocksDB state backend cho state lớn, tăng cường bảo mật/secrets management, bổ sung Prometheus metrics và cải tiến sink để đạt guarantee chặt chẽ hơn.

## 6. TÀI LIỆU THAM KHẢO

[1] Apache Flink Documentation. "Stateful Stream Processing", Flink 1.18.1. https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/concepts/stateful-stream-processing/

[2] Apache Kafka Documentation. "Introduction and Concepts". https://kafka.apache.org/documentation/

[3] ClickHouse Documentation. "ReplacingMergeTree table engine". https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree

[4] ONNX Runtime Documentation. "Get Started with ORT for Java". https://onnxruntime.ai/docs/get-started/with-java.html

[5] Grafana Documentation. "Configure data sources". https://grafana.com/docs/grafana/latest/datasources/

[6] Breiman, L. "Random Forests". Machine Learning, 45, 5-32, 2001.

[7] Chen, T., & Guestrin, C. "XGBoost: A Scalable Tree Boosting System". Proceedings of KDD, 2016.

[8] Tài liệu và mã nguồn nội bộ của đề tài: `README.md`, `docs/tong-hop-luong-chay-cau-hinh-va-danh-gia.md`, `flink-job/`, `data-simulator/`, `ml-model/`, `clickhouse/`, `grafana/`.
