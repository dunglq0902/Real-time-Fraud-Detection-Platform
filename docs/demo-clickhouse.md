# Demo ClickHouse - Kiem tra du lieu Fraud Detection

Tai lieu nay dung cho phan demo "Query ClickHouse de kiem tra du lieu". Muc tieu la chung minh du lieu da di qua pipeline:

```text
Data Simulator -> Kafka -> Flink -> ClickHouse -> Grafana
```

Khi demo, khong can chay tat ca query. Neu thoi gian ngan, uu tien cac query 1, 2, 3 va 5.

## 0. Lenh chay query

Neu dang chay bang Docker Compose, dung mau lenh sau:

```bash
docker exec clickhouse clickhouse-client --password clickhouse123 --query "SELECT 1"
```

Neu query dai, co the dung ClickHouse client trong container:

```bash
docker exec -it clickhouse clickhouse-client --password clickhouse123
```

Sau do paste tung cau SQL ben duoi.

## 1. Kiem tra ClickHouse da co du lieu

Muc tieu demo: xac nhan Flink da ghi ket qua xu ly vao bang `fraud_detection.transactions`.

```sql
SELECT
    count() AS total_transactions,
    min(event_time) AS first_event,
    max(event_time) AS last_event
FROM fraud_detection.transactions;
```

Noi khi demo:

```text
Query dau tien dung de kiem tra bang transactions da co du lieu hay chua. Neu total_transactions tang theo thoi gian, nghia la simulator dang day event vao Kafka, Flink dang xu ly va ClickHouse dang nhan ket qua.
```

Nen quan sat:

- `total_transactions` lon hon 0.
- `last_event` gan thoi diem hien tai khi pipeline dang chay.

## 2. Xem cac giao dich moi nhat

Muc tieu demo: cho thay moi transaction da duoc gan quyet dinh cuoi cung.

```sql
SELECT
    transaction_id,
    account_id,
    amount,
    event_type,
    decision,
    decision_source,
    ml_score,
    combined_score,
    ground_truth_is_fraud,
    decided_at
FROM fraud_detection.transactions
ORDER BY decided_at DESC
LIMIT 10;
```

Noi khi demo:

```text
O day em khong chi luu giao dich goc, ma con luu ket qua sau khi Flink xu ly. Moi dong co decision la APPROVE, ALERT hoac BLOCK, co decision_source de biet quyet dinh den tu Rules Engine, CEP Engine hay ML Scoring.
```

Nen quan sat:

- `decision`: `APPROVE`, `ALERT`, hoac `BLOCK`.
- `decision_source`: `NONE`, `RULES_ENGINE`, `CEP_ENGINE`, hoac `ML_SCORING`.
- `ml_score` va `combined_score`: diem rui ro sau xu ly.

## 3. Thong ke APPROVE / ALERT / BLOCK

Muc tieu demo: chung minh Decision Engine da phan loai giao dich.

```sql
SELECT
    decision,
    count() AS count
FROM fraud_detection.transactions
GROUP BY decision
ORDER BY count DESC;
```

Noi khi demo:

```text
Query nay tong hop dau ra nghiep vu cua he thong. Da so giao dich se la APPROVE, con cac giao dich rui ro hon se vao ALERT hoac BLOCK.
```

Nen quan sat:

- `APPROVE` thuong chiem nhieu nhat.
- `ALERT` va `BLOCK` xuat hien khi simulator sinh fraud hoac rule/CEP/ML danh dau rui ro.

## 4. Kiem tra quyet dinh den tu engine nao

Muc tieu demo: the hien tinh explainability cua he thong.

```sql
SELECT
    decision_source,
    count() AS count
FROM fraud_detection.transactions
GROUP BY decision_source
ORDER BY count DESC;
```

Noi khi demo:

```text
Cot decision_source giup giai thich vi sao he thong dua ra quyet dinh. Neu source la RULES_ENGINE thi giao dich vi pham rule ro rang. Neu la CEP_ENGINE thi do match chuoi hanh vi bat thuong. Neu la ML_SCORING thi do diem ML cao.
```

Nen quan sat:

- `NONE`: giao dich binh thuong.
- `RULES_ENGINE`: bi rule danh dau, vi du amount cao hoac blacklist.
- `CEP_ENGINE`: match pattern hanh vi nhu micro transactions, velocity attack, impossible travel.
- `ML_SCORING`: diem ML vuot nguong.

## 5. Xem alert that trong bang `fraud_alerts`

Muc tieu demo: cho thay he thong co luu alert chi tiet, khong chi luu decision.

```sql
SELECT
    alert_type,
    alert_source,
    severity,
    count() AS count,
    round(avg(score), 3) AS avg_score
FROM fraud_detection.fraud_alerts
GROUP BY alert_type, alert_source, severity
ORDER BY count DESC;
```

Noi khi demo:

```text
Bang fraud_alerts luu cac canh bao chi tiet. O day co the thay loai alert, nguon phat hien, muc do severity va diem rui ro trung binh. Day la du lieu dung cho dashboard va cho nguoi van hanh dieu tra.
```

Nen quan sat:

- `alert_type`: vi du `ACCOUNT_TAKEOVER`, `MICRO_TRANSACTIONS`, `VELOCITY_ATTACK`, `IMPOSSIBLE_TRAVEL`.
- `alert_source`: thuong la `CEP_ENGINE`, `RULES_ENGINE`, hoac `ML_SCORING`.
- `severity`: `MEDIUM`, `HIGH`, `CRITICAL`.

## 6. Xem chi tiet cac alert moi nhat

Muc tieu demo: mo mot vai alert cu the de giai thich cho nguoi nghe.

```sql
SELECT
    alert_time,
    transaction_id,
    account_id,
    alert_type,
    alert_source,
    severity,
    score,
    details
FROM fraud_detection.fraud_alerts
ORDER BY alert_time DESC
LIMIT 10;
```

Noi khi demo:

```text
Query nay di vao tung alert cu the. Truong details cho biet ly do alert, vi du nhieu giao dich nho trong thoi gian ngan, nhay vi tri dia ly qua xa, hoac pattern takeover.
```

Nen quan sat:

- `details` la phan de giai thich ly do.
- `transaction_id` giup lien ket alert voi transaction goc.

## 7. Kiem tra latency end-to-end

Muc tieu demo: chung minh he thong xu ly realtime va do duoc latency.

```sql
SELECT
    round(avg(dateDiff('millisecond', produced_at, decided_at)), 2) AS avg_latency_ms,
    quantile(0.95)(dateDiff('millisecond', produced_at, decided_at)) AS p95_latency_ms,
    quantile(0.99)(dateDiff('millisecond', produced_at, decided_at)) AS p99_latency_ms
FROM fraud_detection.transactions
WHERE produced_at > '1970-01-02'
  AND decided_at > '1970-01-02';
```

Noi khi demo:

```text
Latency duoc tinh tu produced_at, tuc luc simulator gui event, den decided_at, tuc luc Flink tao quyet dinh cuoi. Chi so avg, p95 va p99 giup danh gia he thong realtime co on dinh hay khong.
```

Nen quan sat:

- `avg_latency_ms`: do tre trung binh.
- `p95_latency_ms` va `p99_latency_ms`: do tre trong cac truong hop cham hon.

## 8. So sanh ground truth voi ket qua phat hien

Muc tieu demo: danh gia chat luong detection tren du lieu simulator co nhan.

```sql
SELECT
    count() AS total,
    sum(ground_truth_is_fraud) AS real_fraud,
    sum(is_fraud) AS detected_fraud,
    countIf(ground_truth_is_fraud = 1 AND is_fraud = 1) AS true_positive,
    countIf(ground_truth_is_fraud = 0 AND is_fraud = 1) AS false_positive,
    countIf(ground_truth_is_fraud = 1 AND is_fraud = 0) AS false_negative
FROM fraud_detection.transactions;
```

Noi khi demo:

```text
Vi day la du lieu simulator nen minh co ground truth. Query nay so sanh fraud that voi fraud ma he thong phat hien, tu do tinh duoc true positive, false positive va false negative.
```

Nen quan sat:

- `real_fraud`: so giao dich fraud that do simulator gan nhan.
- `detected_fraud`: so giao dich he thong danh dau fraud.
- `true_positive`: fraud that va he thong bat dung.
- `false_positive`: giao dich binh thuong nhung bi bao fraud.
- `false_negative`: fraud that nhung he thong bo sot.

## 9. Tinh nhanh Precision va Recall

Muc tieu demo: neu bi hoi sau ve metric, co the chay query nay.

```sql
SELECT
    tp,
    fp,
    fn,
    round(tp / nullIf(tp + fp, 0), 4) AS precision,
    round(tp / nullIf(tp + fn, 0), 4) AS recall
FROM
(
    SELECT
        countIf(ground_truth_is_fraud = 1 AND is_fraud = 1) AS tp,
        countIf(ground_truth_is_fraud = 0 AND is_fraud = 1) AS fp,
        countIf(ground_truth_is_fraud = 1 AND is_fraud = 0) AS fn
    FROM fraud_detection.transactions
);
```

Noi khi demo:

```text
Precision cho biet trong cac giao dich bi bao fraud, bao nhieu la fraud that. Recall cho biet trong tong so fraud that, he thong bat duoc bao nhieu.
```

Luu y khi noi:

```text
Metric nay chi co y nghia trong moi truong demo vi simulator co ground truth. Voi du lieu production, ground truth thuong den muon tu qua trinh dieu tra hoac chargeback.
```

## 10. Kiem tra throughput theo materialized view

Muc tieu demo: cho thay ClickHouse co materialized view phuc vu dashboard.

```sql
SELECT
    window_start,
    countMerge(tx_count) AS tx_count,
    round(sumMerge(total_amount), 2) AS total_amount,
    sumIfMerge(fraud_count) AS fraud_count
FROM fraud_detection.tx_throughput
GROUP BY window_start
ORDER BY window_start DESC
LIMIT 10;
```

Noi khi demo:

```text
Ngoai bang goc, ClickHouse con co materialized view tong hop throughput theo phut. Grafana co the doc cac bang aggregate nhu the nay de query nhanh hon.
```

Nen quan sat:

- Moi dong la mot cua so 1 phut.
- `tx_count` cho biet so transaction trong phut do.
- `fraud_count` cho biet so transaction bi danh dau fraud trong phut do.

## 11. Demo ngan trong 2 phut

Neu chi co khoang 2 phut, chay theo thu tu nay:

1. Kiem tra co du lieu:

```sql
SELECT count() FROM fraud_detection.transactions;
```

2. Xem giao dich moi nhat:

```sql
SELECT transaction_id, decision, decision_source, ml_score, combined_score, decided_at
FROM fraud_detection.transactions
ORDER BY decided_at DESC
LIMIT 5;
```

3. Thong ke decision:

```sql
SELECT decision, count()
FROM fraud_detection.transactions
GROUP BY decision
ORDER BY count() DESC;
```

4. Thong ke alert:

```sql
SELECT alert_type, severity, count()
FROM fraud_detection.fraud_alerts
GROUP BY alert_type, severity
ORDER BY count() DESC;
```

Loi noi tong ket:

```text
Bon query nay chung minh du lieu da duoc ghi vao ClickHouse, tung giao dich da co quyet dinh, he thong co thong ke APPROVE/ALERT/BLOCK va co alert chi tiet cho cac truong hop rui ro.
```

## 12. Luu y khi demo

- Neu query tra ve 0 record, cho pipeline chay them vai giay hoac kiem tra Flink job dang `RUNNING`.
- Bang `transactions` dung `ReplacingMergeTree(decided_at)`, nen trong mot so tinh huong retry co the co duplicate tam thoi truoc khi ClickHouse merge.
- Neu can ket qua dedup chat hon khi demo, co the them `FINAL`:

```sql
SELECT count()
FROM fraud_detection.transactions FINAL;
```

- Khong nen noi ClickHouse sink la exactly-once tuyet doi. Cach noi dung hon la: Flink source/state co exactly-once theo checkpoint, con ClickHouse sink dat near exactly-once nho checkpoint buffer va dedup bang `ReplacingMergeTree`.
