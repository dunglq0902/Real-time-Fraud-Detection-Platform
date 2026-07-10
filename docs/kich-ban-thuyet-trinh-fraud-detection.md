# Kịch bản thuyết trình chi tiết: Real-Time Fraud Detection Platform

Tài liệu này mở rộng từ file `docs/noi-dung-slide-fraud-detection.md`, dùng để luyện nói hoặc làm speaker notes khi trình bày slide.

Thời lượng gợi ý: 18-25 phút.  
Giọng trình bày: rõ ràng, đi theo luồng từ bài toán -> kiến trúc -> xử lý realtime -> phát hiện gian lận -> dashboard -> đánh giá.  
Vai xưng hô trong script: "em" để phù hợp với bối cảnh bảo vệ đồ án.

## Slide 1: Tiêu đề

Script:

Em xin chào thầy cô và các bạn. Hôm nay em xin trình bày project Real-Time Fraud Detection Platform, tức là một nền tảng phát hiện gian lận giao dịch theo thời gian thực.

Ý tưởng chính của hệ thống là mô phỏng một pipeline giống với bài toán trong ngân hàng hoặc ví điện tử. Khi giao dịch phát sinh liên tục, hệ thống cần đọc dữ liệu ngay lập tức, phân tích rủi ro và đưa ra quyết định là cho qua, cảnh báo, hoặc chặn giao dịch.

Trong project này, em sử dụng Kafka để nhận luồng dữ liệu, Apache Flink để xử lý realtime, kết hợp ba lớp phát hiện gồm Rules Engine, CEP và Machine Learning. Kết quả cuối cùng được lưu vào ClickHouse và quan sát bằng Grafana.

Câu chuyển ý:

Trước khi đi vào kiến trúc, em sẽ nói ngắn gọn về bối cảnh vì sao bài toán này cần xử lý realtime.

## Slide 2: Bối cảnh bài toán

Script:

Trong các hệ thống tài chính, giao dịch có thể phát sinh với tần suất rất lớn và gần như liên tục. Mỗi giao dịch có nhiều thông tin đi kèm như tài khoản, số tiền, merchant, vị trí, kênh giao dịch và thời điểm phát sinh.

Với gian lận giao dịch, thời gian phản ứng là yếu tố rất quan trọng. Nếu mình chỉ phân tích dữ liệu theo batch sau vài giờ hoặc cuối ngày, thì giao dịch gian lận có thể đã hoàn tất, tiền đã bị chuyển đi và việc xử lý sau đó sẽ khó hơn rất nhiều.

Ngoài tốc độ, hệ thống cũng cần giải thích được quyết định. Ví dụ khi một giao dịch bị block, mình cần biết lý do là vì số tiền quá lớn, merchant nằm trong blacklist, có chuỗi đăng nhập bất thường, hay do điểm ML score cao.

Vì vậy, bài toán đặt ra không chỉ là "dự đoán fraud hay không fraud", mà là xây dựng một pipeline realtime có khả năng xử lý nhanh, phát hiện đa dạng kiểu gian lận và cung cấp lý do rõ ràng cho từng quyết định.

Câu chuyển ý:

Từ bối cảnh đó, em xác định các mục tiêu chính của hệ thống như sau.

## Slide 3: Mục tiêu hệ thống

Script:

Mục tiêu đầu tiên là tạo được một luồng giao dịch liên tục để mô phỏng môi trường thực tế. Phần này được thực hiện bởi Data Simulator, có nhiệm vụ sinh ra các transaction và một số rule update.

Mục tiêu thứ hai là đưa toàn bộ dữ liệu vào Kafka. Kafka đóng vai trò như một event queue, giúp tách phần sinh dữ liệu khỏi phần xử lý. Nhờ vậy, Flink có thể đọc stream với tốc độ ổn định và có khả năng replay dữ liệu khi cần.

Mục tiêu thứ ba là xử lý realtime bằng Apache Flink. Đây là thành phần trung tâm, chịu trách nhiệm lọc dữ liệu, áp dụng rule, phát hiện pattern bằng CEP, tạo feature cho ML, chấm điểm rủi ro và đưa ra quyết định cuối.

Điểm quan trọng nhất là hệ thống không dựa vào một phương pháp duy nhất. Em kết hợp ba lớp phát hiện: Rules để bắt các điều kiện rõ ràng, CEP để bắt chuỗi hành vi theo thời gian, và ML để đánh giá rủi ro dựa trên feature.

Cuối cùng, kết quả được lưu vào ClickHouse và hiển thị trên Grafana để có thể theo dõi số lượng giao dịch, alert, latency, throughput và hiệu quả phát hiện gian lận.

Câu chuyển ý:

Sau khi xử lý, hệ thống cần trả về một kết quả nghiệp vụ rõ ràng cho từng giao dịch.

## Slide 4: Kết quả đầu ra nghiệp vụ

Script:

Đầu ra của hệ thống được chuẩn hóa thành ba quyết định chính: APPROVE, ALERT và BLOCK.

APPROVE nghĩa là giao dịch được xem là bình thường và có thể cho qua. Đây là trạng thái dành cho các giao dịch không vi phạm rule, không match pattern CEP nguy hiểm và có ML score thấp.

ALERT nghĩa là giao dịch có dấu hiệu nghi ngờ. Với loại này, hệ thống chưa nhất thiết chặn ngay, nhưng cần ghi nhận để theo dõi, điều tra hoặc đưa vào case management.

BLOCK là mức nghiêm trọng nhất. Quyết định này áp dụng cho giao dịch có rủi ro cao, ví dụ merchant nằm trong blacklist, pattern CEP rất nguy hiểm, hoặc ML score vượt ngưỡng cao.

Ngoài quyết định cuối cùng, hệ thống còn lưu `decision_source`. Trường này cho biết quyết định đến từ Rules Engine, CEP Engine hay ML Scoring. Đây là phần quan trọng để hệ thống có khả năng giải thích, nhất là khi cần bảo vệ quyết định trước người vận hành hoặc người dùng.

Câu chuyển ý:

Để tạo ra các quyết định này, hệ thống được thiết kế theo kiến trúc realtime end-to-end.

## Slide 5: Kiến trúc tổng thể

Script:

Đây là kiến trúc tổng thể của hệ thống. Luồng chính bắt đầu từ Data Simulator. Simulator tạo transaction và rule update, sau đó đẩy vào Kafka.

Kafka là lớp trung gian nhận dữ liệu đầu vào. Transaction sẽ được gửi vào topic `transactions`, rule update gửi vào topic `fraud-rules`, còn kết quả cảnh báo cuối cùng có thể được ghi ra topic `fraud-alerts`.

Flink là nơi xử lý chính. Flink đọc transaction từ Kafka, áp dụng Rules Engine, CEP Engine, ML Scoring và Decision Engine. Sau khi có kết quả, Flink ghi dữ liệu sang ClickHouse để lưu trữ phân tích, đồng thời có thể ghi alert trở lại Kafka.

ClickHouse được dùng vì phù hợp với truy vấn phân tích theo thời gian, ví dụ count transaction, group by decision, tính latency hoặc thống kê alert type. Grafana đọc dữ liệu từ ClickHouse để tạo dashboard realtime.

MinIO có hai vai trò. Thứ nhất là lưu model ONNX để Flink tải vào khi chạy ML inference. Thứ hai là lưu checkpoint hoặc savepoint, giúp Flink có khả năng khôi phục state khi job gặp lỗi.

Câu chuyển ý:

Tiếp theo, em sẽ tóm tắt vai trò của từng công nghệ trong kiến trúc này.

## Slide 6: Vai trò từng công nghệ

Script:

Trong hệ thống, mỗi công nghệ được chọn cho một vai trò cụ thể.

Kafka chịu trách nhiệm event streaming. Nó giúp buffer dữ liệu, tách producer và consumer, đồng thời cho phép replay lại event khi cần. Đây là thành phần rất phổ biến trong các hệ thống realtime.

Flink là engine xử lý stream có state. Nghĩa là Flink không chỉ xử lý từng event độc lập, mà còn có thể lưu trạng thái theo account để tính toán các hành vi gần đây, ví dụ số giao dịch trong một giờ hoặc tổng số tiền trong một cửa sổ thời gian.

CEP, hay Complex Event Processing, dùng để phát hiện chuỗi hành vi. Đây là điểm khác biệt so với rule đơn lẻ. Ví dụ một lần login failed chưa chắc là fraud, nhưng nhiều lần failed login, sau đó login success rồi transfer thì là pattern đáng ngờ.

ONNX Runtime giúp chạy model ML trực tiếp trong JVM của Flink. ClickHouse lưu dữ liệu phân tích tốc độ cao, Grafana hiển thị dashboard, còn MinIO đóng vai trò object storage cho model và checkpoint.

Câu chuyển ý:

Bây giờ em sẽ đi vào tầng Kafka, nơi các luồng dữ liệu được tổ chức thành các topic.

## Slide 7: Kafka topics

Script:

Trong project này, Kafka được chia thành ba topic chính.

Topic đầu tiên là `transactions`, dùng để nhận giao dịch đầu vào. Topic này có 8 partitions vì transaction là luồng có tải cao nhất. Khi tăng partition, Flink có thể tăng parallelism để xử lý nhiều event hơn.

Topic thứ hai là `fraud-rules`, dùng để nhận các rule gian lận dạng JSON. Topic này chỉ cần 1 partition vì rule update không nhiều như transaction. Ngoài ra, dùng 1 partition giúp giữ thứ tự update rule đơn giản hơn.

Topic thứ ba là `fraud-alerts`, dùng để lưu hoặc phát tán alert cuối cùng sau Decision Engine. Topic này giúp hệ thống dễ mở rộng sang các thành phần khác, ví dụ gửi SMS, email, push notification hoặc tạo case điều tra.

Một điểm cần chú ý là transaction dùng key theo `account_id`. Cách này giúp các event của cùng một tài khoản có xu hướng được xử lý nhất quán hơn, rất hữu ích khi cần state theo account hoặc pattern theo chuỗi hành vi.

Câu chuyển ý:

Nguồn dữ liệu gửi vào Kafka trong project này đến từ Data Simulator.

## Slide 8: Data Simulator

Script:

Data Simulator có nhiệm vụ tạo dữ liệu giả lập cho hệ thống. Điểm quan trọng là simulator không chỉ random một vài field đơn giản, mà cố gắng tạo dữ liệu có cấu trúc gần với giao dịch ngân hàng.

Simulator tạo khoảng 500 account và 200 merchant. Mỗi event có thể thuộc nhiều kênh khác nhau như ATM, ONLINE, POS hoặc MOBILE. Event type cũng đa dạng, gồm LOGIN, TRANSFER, PAYMENT, WITHDRAWAL, LOGIN_FAILED và LOGIN_SUCCESS.

Về vị trí, dữ liệu có cả địa điểm ở Việt Nam và quốc tế. Điều này giúp kiểm tra các pattern liên quan đến vị trí, ví dụ impossible travel, khi cùng một account xuất hiện ở hai nơi rất xa nhau trong thời gian ngắn.

Trong cấu hình demo, simulator có thể tạo khoảng 100 events mỗi giây, với fraud ratio khoảng 1%. Tỷ lệ này giúp dữ liệu có phần lớn giao dịch bình thường, xen kẽ một lượng nhỏ gian lận để kiểm tra khả năng phát hiện của hệ thống.

Câu chuyển ý:

Mỗi event giao dịch được thiết kế với schema đủ thông tin cho cả rule, CEP và ML.

## Slide 9: Schema giao dịch

Script:

Mỗi transaction có các field định danh như `transaction_id`, `account_id` và `card_id`. Các field này giúp hệ thống biết giao dịch nào đang được xử lý, thuộc tài khoản nào và có thể dùng để dedup khi lưu trữ.

Các field nghiệp vụ gồm `amount`, `event_type`, `channel`, `location`, `merchant_id` và `status`. Đây là dữ liệu chính để rule và CEP đánh giá rủi ro. Ví dụ `amount` dùng cho rule số tiền lớn, `merchant_id` dùng để kiểm tra blacklist, còn `location` dùng cho impossible travel.

Hai trường thời gian rất quan trọng là `timestamp` và `produced_at`. `timestamp` là thời điểm nghiệp vụ của event, nên được dùng làm event time trong Flink. `produced_at` là thời điểm simulator gửi event, được dùng để tính end-to-end latency.

Ngoài ra có trường `is_fraud` từ simulator. Trong production thực tế, mình không có nhãn fraud ngay lập tức. Nhưng trong môi trường demo, nhãn này giúp đánh giá hệ thống bằng các metric như FDR, FPR và Precision.

Câu chuyển ý:

Từ schema này, simulator có thể tạo ra nhiều kiểu fraud khác nhau.

## Slide 10: Các kiểu gian lận giả lập

Script:

Simulator tạo nhiều dạng gian lận để kiểm tra từng lớp phát hiện.

Kiểu đầu tiên là account takeover. Đây là tình huống tài khoản bị chiếm quyền, thường có chuỗi nhiều lần login failed, sau đó login success và tiếp theo là transfer. Pattern này phù hợp để phát hiện bằng CEP.

Kiểu thứ hai là rapid transactions hoặc velocity attack. Nghĩa là một tài khoản thực hiện nhiều giao dịch liên tiếp trong thời gian rất ngắn. Nếu chỉ nhìn từng giao dịch riêng lẻ thì có thể không thấy bất thường, nhưng nhìn theo chuỗi thì rủi ro cao hơn.

Kiểu thứ ba là impossible travel. Ví dụ một tài khoản vừa có giao dịch ở Việt Nam, sau vài phút lại có giao dịch ở một quốc gia rất xa. Về mặt vật lý, người dùng gần như không thể di chuyển nhanh như vậy.

Ngoài ra còn có high amount, tức giao dịch số tiền lớn, và micro transactions, tức nhiều giao dịch nhỏ liên tục. Các loại này giúp kiểm tra cả rule đơn giản, CEP theo window và feature cho ML.

Câu chuyển ý:

Sau khi dữ liệu được tạo và đưa vào Kafka, phần xử lý chính diễn ra trong Flink job.

## Slide 11: Flink job tổng quan

Script:

Flink job là trung tâm xử lý realtime của toàn bộ hệ thống. Luồng xử lý bắt đầu từ Kafka source đọc transaction.

Đầu tiên, hệ thống lọc các transaction không hợp lệ. Sau đó transaction đi qua Rules Engine để kiểm tra các rule rõ ràng như amount vượt ngưỡng hoặc merchant blacklist.

Song song với đó, dữ liệu được đưa qua CEP Pattern Detector để phát hiện các chuỗi hành vi theo account. Alert từ CEP sau đó được merge lại với transaction gốc bằng CepAlertMerger.

Tiếp theo, FeatureAggregator tạo ra các feature hành vi gần đây theo account, ví dụ tần suất giao dịch, tổng amount hoặc số merchant khác nhau. Các feature này được đưa vào ONNX ML Scorer để tính ML score.

Cuối cùng, DecisionMaker tổng hợp kết quả từ Rules, CEP và ML để đưa ra APPROVE, ALERT hoặc BLOCK. Kết quả được ghi sang ClickHouse và topic `fraud-alerts`.

Trong demo, job chạy parallelism 8, checkpoint mỗi 60 giây và lưu state theo account để hỗ trợ xử lý có trạng thái.

Câu chuyển ý:

Một điểm quan trọng trong Flink job là hệ thống xử lý theo event time, không chỉ theo thời gian server nhận event.

## Slide 12: Event time và watermark

Script:

Trong stream processing, event có thể đến không đúng thứ tự. Ví dụ giao dịch A xảy ra trước giao dịch B, nhưng vì network hoặc partition Kafka, B có thể đến Flink trước A.

Nếu hệ thống chỉ dùng processing time, tức thời gian Flink nhận event, thì các pattern theo thời gian có thể bị đánh sai. Đặc biệt với CEP, khái niệm "trong 2 phút", "trong 10 phút" hoặc "trong 15 phút" cần dựa trên thời gian nghiệp vụ của event.

Vì vậy, hệ thống dùng `timestamp` của transaction làm event time. Watermark được cấu hình cho phép out-of-order tối đa 5 giây. Nghĩa là Flink chấp nhận event đến muộn trong một khoảng nhỏ để tăng độ chính xác.

Ngoài ra, idleness 30 giây giúp tránh trường hợp watermark bị kẹt khi một partition không có dữ liệu. Đây là cấu hình quan trọng khi Kafka có nhiều partition và tải không phân bố đều.

Câu chuyển ý:

Sau phần nền tảng về event time, em sẽ đi vào lớp phát hiện đầu tiên là Dynamic Rules Engine.

## Slide 13: Lớp phát hiện 1 - Dynamic Rules Engine

Script:

Rules Engine là lớp phát hiện đầu tiên và dễ giải thích nhất. Lớp này phù hợp với các điều kiện rõ ràng, có thể diễn đạt bằng rule nghiệp vụ.

Ví dụ, nếu amount lớn hơn 10.000 thì đánh dấu HIGH. Nếu merchant nằm trong blacklist thì đánh dấu CRITICAL. Hoặc nếu có giao dịch chuyển tiền lớn vào ban đêm thì đánh dấu MEDIUM.

Điểm quan trọng là rule không bị hard-code cố định trong Flink job. Thay vào đó, rule được gửi vào Kafka topic `fraud-rules` dưới dạng JSON. Flink đọc topic này và cập nhật rule trong runtime.

Để mọi subtask của Flink đều nhận được cùng bộ rule, hệ thống dùng Broadcast State. Khi có rule mới, rule được broadcast đến tất cả task đang xử lý transaction.

Rules Engine cũng hỗ trợ các operator cơ bản như GREATER_THAN, LESS_THAN, EQUALS, IN và NOT_IN. Nhờ vậy, mình có thể mô tả nhiều điều kiện nghiệp vụ mà không cần sửa code xử lý chính.

Câu chuyển ý:

Việc dùng Broadcast State là một lựa chọn thiết kế quan trọng, nên em sẽ giải thích thêm ở slide tiếp theo.

## Slide 14: Vì sao dùng Broadcast State?

Script:

Trong Flink, khi job chạy parallelism 8, sẽ có nhiều subtask cùng xử lý transaction. Mỗi subtask xử lý một phần luồng dữ liệu.

Vấn đề là tất cả các subtask này đều cần cùng một bộ rule mới nhất. Nếu một task dùng rule cũ còn task khác dùng rule mới, kết quả phát hiện sẽ không nhất quán.

Một cách đơn giản nhưng không tốt là mỗi event lại query database để lấy rule. Tuy nhiên, transaction volume cao nên cách này gây tốn tài nguyên, tăng latency và tạo phụ thuộc vào database bên ngoài.

Broadcast State giải quyết vấn đề này tốt hơn. Rule là dữ liệu tương đối nhỏ, update không quá thường xuyên, nên có thể broadcast đến mọi subtask và lưu trong state. Khi transaction đến, task có thể kiểm tra rule ngay trong memory hoặc state cục bộ.

Ngoài ra, hệ thống có thể cache rule đã parse để tránh parse JSON lặp lại cho từng event. Điều này giúp giảm overhead trong đường xử lý realtime.

Câu chuyển ý:

Rules phát hiện tốt các điều kiện đơn lẻ, nhưng nhiều gian lận cần nhìn theo chuỗi hành vi. Vì vậy hệ thống có lớp CEP.

## Slide 15: Lớp phát hiện 2 - CEP Engine

Script:

CEP là viết tắt của Complex Event Processing. Khác với rule đơn lẻ, CEP dùng để phát hiện các chuỗi sự kiện có thứ tự và xảy ra trong một cửa sổ thời gian nhất định.

Trong fraud detection, nhiều hành vi nguy hiểm không thể kết luận từ một transaction riêng lẻ. Ví dụ một lần login failed có thể chỉ là người dùng nhập sai mật khẩu. Nhưng nếu có nhiều lần failed login, sau đó login success và ngay lập tức transfer, thì khả năng account takeover cao hơn nhiều.

Để pattern được xét đúng theo từng tài khoản, input của CEP được `keyBy(accountId)`. Điều này nghĩa là các event của cùng một account sẽ được gom theo logic xử lý riêng, tránh trộn hành vi của nhiều người dùng khác nhau.

Trong hệ thống này, CEP tập trung vào các pattern chính như Account Takeover, Micro Transactions, Velocity Attack và Impossible Travel. Mỗi pattern có điều kiện, window và severity riêng.

Câu chuyển ý:

Em sẽ đi cụ thể hơn vào từng pattern CEP ở slide tiếp theo.

## Slide 16: CEP Pattern chi tiết

Script:

Pattern đầu tiên là Account Takeover. Điều kiện là có ít nhất 3 lần login failed, sau đó login success, rồi có transfer trong vòng 10 phút. Pattern này có severity HIGH hoặc CRITICAL vì nó mô phỏng tình huống tài khoản vừa bị dò mật khẩu và sau đó chuyển tiền.

Pattern thứ hai là Micro Transactions. Đây là trường hợp có nhiều giao dịch nhỏ, ví dụ từ 5 giao dịch dưới 10 đơn vị tiền trong 2 phút. Hành vi này có thể liên quan đến việc test thẻ, chia nhỏ giao dịch hoặc thử hệ thống.

Pattern thứ ba là Velocity Attack. Hệ thống phát hiện nhiều transfer hoặc payment có số tiền trung bình, xảy ra rất nhanh trong khoảng 2 giây. Điểm đáng ngờ ở đây là tốc độ, không nhất thiết là số tiền của từng giao dịch.

Pattern thứ tư là Impossible Travel. Hệ thống so sánh hai location của cùng một account. Nếu khoảng cách từ 150km trở lên trong vòng 15 phút, hệ thống đánh dấu rủi ro. Việc tính khoảng cách có thể dùng công thức Haversine dựa trên tọa độ.

Các pattern này bổ sung cho Rules Engine. Rules bắt những điều kiện rõ ràng tại một thời điểm, còn CEP bắt hành vi kéo dài theo chuỗi thời gian.

Câu chuyển ý:

Vì CEP chạy như một nhánh xử lý riêng, hệ thống cần cơ chế merge alert CEP về đúng transaction gốc.

## Slide 17: CepAlertMerger

Script:

Trong pipeline, main stream của transaction có thể đi rất nhanh từ Kafka source qua các operator tiếp theo. Trong khi đó, nhánh CEP cần thêm thời gian để match pattern, đặc biệt với các pattern có nhiều event.

Nếu không xử lý cẩn thận, transaction gốc có thể đi tới DecisionMaker và bị approve trước khi alert từ CEP được tạo ra. Khi đó hệ thống sẽ bỏ lỡ tín hiệu rủi ro từ CEP.

Để giải quyết, hệ thống dùng CepAlertMerger. Ý tưởng là buffer transaction trong một khoảng rất ngắn, khoảng 10ms, để chờ xem có alert CEP tương ứng hay không.

Nếu CEP alert đến kịp, alert sẽ được gắn vào transaction trước khi transaction đi vào DecisionMaker. Nếu không có alert, transaction tiếp tục đi qua như bình thường.

Thiết kế này là một trade-off. Mình chấp nhận thêm một độ trễ rất nhỏ để tăng khả năng gắn đúng tín hiệu CEP vào quyết định cuối cùng.

Câu chuyển ý:

Sau Rules và CEP, lớp ML cần thêm feature hành vi, vì model không chỉ nhìn một event riêng lẻ.

## Slide 18: Feature Aggregator

Script:

Feature Aggregator là bước chuẩn bị dữ liệu cho ML Scoring. Mục tiêu là biến lịch sử hành vi gần đây của tài khoản thành các feature có ý nghĩa.

Flink dùng Keyed State theo `account_id`. Nghĩa là với mỗi tài khoản, hệ thống có thể lưu một phần lịch sử gần đây để tính toán thống kê.

Ví dụ, hệ thống có thể tính transaction frequency trong 1 giờ, tổng số tiền giao dịch trong 1 giờ, average amount, số merchant khác nhau hoặc mức độ thay đổi vị trí. Các feature này phản ánh hành vi của account tốt hơn là chỉ nhìn một transaction đơn lẻ.

Điểm quan trọng là state được checkpoint. Nếu Flink job bị lỗi và restart, state có thể được khôi phục từ checkpoint, tránh mất toàn bộ ngữ cảnh hành vi đã tích lũy.

Feature Aggregator giúp ML model có thêm dữ liệu ngữ cảnh. Nhờ vậy, model có thể đánh giá rủi ro dựa trên cả giao dịch hiện tại và hành vi gần đây.

Câu chuyển ý:

Từ các feature đó, hệ thống chạy model ML đã được export sang định dạng ONNX.

## Slide 19: Lớp phát hiện 3 - ML Scoring với ONNX

Script:

Lớp phát hiện thứ ba là ML Scoring. Trong project này, model được train ở module `ml-trainer`, sau đó export sang định dạng ONNX.

ONNX là định dạng giúp model có thể chạy ở nhiều môi trường khác nhau. Thay vì phải triển khai một model serving service riêng, Flink có thể tải model ONNX và chạy inference trực tiếp trong JVM bằng ONNX Runtime.

Model được lưu trên MinIO. Khi Flink job khởi động, OnnxModelScorer tải model về và dùng model đó để tính ML score cho từng transaction sau khi đã có feature.

Ưu điểm của cách này là latency thấp hơn, vì Flink không cần gọi network sang một service ML riêng cho từng event. Điều này phù hợp với yêu cầu realtime.

Tuy nhiên, cách này cũng có trade-off. Nếu muốn update model, hệ thống cần cơ chế reload, restart hoặc model versioning. Đây là một hướng có thể phát triển thêm nếu đưa hệ thống gần hơn với production.

Câu chuyển ý:

Sau khi có tín hiệu từ Rules, CEP và ML, hệ thống cần một nơi tổng hợp để ra quyết định cuối.

## Slide 20: Decision Engine

Script:

Decision Engine, cụ thể là DecisionMaker, là bước hợp nhất tất cả tín hiệu phát hiện.

Input của DecisionMaker gồm rule alerts, CEP alerts và ML score. Output là một trong ba quyết định: APPROVE, ALERT hoặc BLOCK.

Logic quyết định được thiết kế theo priority. Nếu có rule hoặc CEP với severity CRITICAL, hệ thống ưu tiên BLOCK, vì đây là tín hiệu rủi ro rất cao. Nếu ML score vượt ngưỡng rất cao, hệ thống cũng có thể BLOCK.

Với các tín hiệu ở mức trung bình hoặc nghi ngờ, hệ thống trả về ALERT. Điều này giúp người vận hành theo dõi mà không nhất thiết chặn ngay mọi giao dịch có dấu hiệu nhẹ.

Nếu không có rule, không có CEP alert và ML score thấp, giao dịch được APPROVE.

Điểm quan trọng là DecisionMaker lưu `decision_source`. Nhờ đó, mỗi kết quả không chỉ là một nhãn, mà còn có thông tin giải thích quyết định đến từ đâu.

Câu chuyển ý:

Để dễ hình dung hơn, em đưa ra một số ví dụ logic quyết định cụ thể.

## Slide 21: Ví dụ logic quyết định

Script:

Ví dụ đầu tiên, nếu merchant là `MERCH-0199` và merchant này nằm trong blacklist, hệ thống có thể BLOCK ngay, với source là RULES_ENGINE. Đây là trường hợp rule rõ ràng và dễ giải thích.

Ví dụ thứ hai, nếu một account có 5 giao dịch nhỏ trong 2 phút, đặc biệt qua nhiều merchant khác nhau, hệ thống có thể ALERT hoặc BLOCK với source là CEP_ENGINE. Ở đây rủi ro không nằm ở từng giao dịch riêng lẻ, mà nằm ở chuỗi hành vi.

Ví dụ thứ ba, nếu amount là 15.000 nhưng ML score chỉ 0.4, hệ thống vẫn có thể ALERT do Rules Engine vì số tiền vượt ngưỡng. Điều này cho thấy rule và ML không thay thế nhau hoàn toàn, mà bổ sung cho nhau.

Ví dụ thứ tư, nếu amount bình thường nhưng ML score là 0.91, hệ thống có thể BLOCK với source là ML_SCORING. Trường hợp này cho thấy ML có thể phát hiện rủi ro dựa trên tổ hợp feature mà rule đơn giản không bắt được.

Cuối cùng, nếu không có rule, không có CEP và ML score thấp, giao dịch được APPROVE. Đây là luồng bình thường của đa số giao dịch.

Câu chuyển ý:

Sau khi có quyết định, hệ thống cần lưu kết quả để phục vụ dashboard và phân tích.

## Slide 22: ClickHouse storage

Script:

ClickHouse được dùng làm lớp lưu trữ phân tích. Lý do chọn ClickHouse là dashboard thường cần các truy vấn dạng count, group by, aggregate theo thời gian, và đây là workload rất phù hợp với columnar database.

Hệ thống có bảng `transactions` để lưu quyết định cuối của từng giao dịch. Bảng này giúp truy vấn tổng số giao dịch, số APPROVE, ALERT, BLOCK hoặc phân tích theo account, channel, merchant.

Bảng `fraud_alerts` lưu các alert cuối cùng. Dữ liệu này phục vụ việc xem loại alert, nguồn phát hiện, severity và các trường giải thích.

Các bảng dùng ReplacingMergeTree để hỗ trợ dedup. Trong stream processing, việc retry có thể làm một record được ghi nhiều lần. Vì vậy thiết kế bảng cần hỗ trợ loại bỏ bản ghi trùng theo khóa như transaction_id.

Ngoài ra, hệ thống có TTL cho dữ liệu và materialized views cho các aggregate thường dùng, ví dụ throughput, alert by type, geo distribution và latency. Điều này giúp Grafana query nhanh hơn.

Câu chuyển ý:

Khi nói về lưu trữ stream, cần trình bày rõ mức đảm bảo exactly-once của từng phần.

## Slide 23: Exactly-once và sink thực tế

Script:

Đây là slide cần trình bày trung thực về guarantee của hệ thống.

Ở phía Kafka source và Flink state, hệ thống có thể đạt exactly-once theo checkpoint. Flink checkpoint mỗi 60 giây, Kafka offset được commit theo checkpoint, và state được lưu vào MinIO.

Điều này nghĩa là khi Flink job restart, nó có thể khôi phục state và offset gần nhất theo checkpoint, tránh xử lý sai lệch quá nhiều trong phần source và stateful processing.

Tuy nhiên, với ClickHouse custom sink, hệ thống không dùng TwoPhaseCommit sink đầy đủ. Vì vậy không nên nói end-to-end tới ClickHouse là exactly-once tuyệt đối.

Cách diễn đạt chính xác hơn là near exactly-once. Hệ thống dựa vào retry, checkpoint buffer và dedup bằng ReplacingMergeTree để giảm trùng lặp và đảm bảo kết quả thực tế ổn định hơn.

Nếu đưa vào production, phần sink có thể được cải tiến bằng transactional sink hoặc cơ chế idempotent chặt hơn.

Câu chuyển ý:

Dữ liệu sau khi lưu vào ClickHouse sẽ được hiển thị trên Grafana để quan sát realtime.

## Slide 24: Grafana dashboard

Script:

Grafana dashboard là phần quan sát vận hành của hệ thống. Nó không nằm trong đường ra quyết định giao dịch, nhưng rất quan trọng để đánh giá hệ thống đang chạy như thế nào.

Dashboard hiển thị tổng số transaction, số fraud detected, số transaction bị block, tổng alert, throughput events per second và latency end-to-end.

Về đánh giá chất lượng phát hiện, dashboard có Ground Truth Fraud Rate, FDR hoặc Recall, FPR và Precision. Ground Truth Fraud Rate là tỷ lệ fraud thật trong dữ liệu simulator. FDR hoặc Recall cho biết trong các fraud thật, hệ thống bắt đúng bao nhiêu phần trăm.

FPR cho biết tỷ lệ giao dịch bình thường bị báo nhầm. Đây là metric rất quan trọng trong thực tế, vì false positive cao sẽ làm phiền người dùng và tăng tải cho đội vận hành.

Precision cho biết trong các giao dịch bị alert hoặc block, bao nhiêu phần trăm thật sự là fraud. Nếu precision thấp, nghĩa là hệ thống cảnh báo nhiều nhưng nhiều cảnh báo không chính xác.

Dashboard cũng có breakdown theo decision, decision source và alert type. Nhờ đó, mình biết hệ thống đang block chủ yếu vì rule, CEP hay ML, và loại fraud nào xuất hiện nhiều nhất.

Câu chuyển ý:

Khi demo, em sẽ dùng dashboard cùng với Flink UI và ClickHouse để chứng minh pipeline chạy end-to-end.

## Slide 25: Demo flow

Script:

Trong phần demo, em chạy toàn bộ stack bằng Docker Compose với lệnh `docker compose up --build -d`.

Sau khi các container khởi động, em kiểm tra các service chính gồm Kafka, Flink, ClickHouse và Grafana. Flink UI có thể mở ở `http://localhost:8081`, còn Grafana mở ở `http://localhost:3000`.

Luồng demo bắt đầu từ simulator gửi transaction vào Kafka. Flink job đọc transaction, xử lý qua Rules, CEP, Feature Aggregator, ML Scoring và DecisionMaker. Sau đó kết quả được ghi vào ClickHouse.

Trên Grafana, em quan sát transaction count, alert count, blocked transaction, throughput và latency. Nếu dashboard thay đổi theo thời gian, điều đó chứng minh dữ liệu đang đi qua pipeline realtime.

Ngoài dashboard, em có thể query nhanh ClickHouse để kiểm tra dữ liệu đã được ghi vào bảng `transactions` hoặc `fraud_alerts`. Việc này giúp xác nhận không chỉ giao diện chạy, mà dữ liệu thật sự đã được xử lý và lưu trữ.

Câu chuyển ý:

Sau phần demo, em sẽ tổng kết các điểm mạnh chính của hệ thống.

## Slide 26: Điểm mạnh của hệ thống

Script:

Điểm mạnh lớn nhất của hệ thống là kiến trúc multi-layer detection. Hệ thống không phụ thuộc hoàn toàn vào ML, cũng không chỉ dùng rule cứng. Rules, CEP và ML bổ sung cho nhau.

Rules giúp bắt các điều kiện rõ ràng và dễ giải thích. CEP giúp phát hiện chuỗi hành vi theo thời gian, ví dụ account takeover hoặc impossible travel. ML giúp đánh giá rủi ro dựa trên tổ hợp feature và hành vi gần đây.

Điểm mạnh thứ hai là explainability. Với các trường như `decision_source`, `rule_triggered` và `ml_score`, hệ thống có thể giải thích vì sao một giao dịch bị alert hoặc block.

Điểm mạnh thứ ba là rule update động. Nhờ Kafka và Broadcast State, hệ thống có thể cập nhật rule mà không cần restart Flink job.

Ngoài ra, hệ thống xử lý stateful realtime theo account, có checkpoint và MinIO cho fault tolerance, có Grafana dashboard để quan sát, và có thể mở rộng bằng cách tăng Kafka partitions cùng Flink parallelism.

Câu chuyển ý:

Bên cạnh điểm mạnh, project hiện tại vẫn có một số hạn chế cần nói rõ.

## Slide 27: Hạn chế và cách trả lời phản biện

Script:

Vì đây là project mô phỏng kiến trúc, hạn chế đầu tiên là dữ liệu hiện tại là synthetic. Dữ liệu do simulator sinh ra giúp kiểm thử end-to-end, nhưng chưa thể đại diện hoàn toàn cho dữ liệu ngân hàng thật.

Hạn chế thứ hai là ground truth cũng đến từ simulator. Nhờ ground truth này, hệ thống có thể tính FDR, FPR và Precision, nhưng khi đưa vào thực tế cần validate thêm bằng dữ liệu thật hoặc public dataset phù hợp.

Một điểm cần chú ý là FPR có thể còn cao và Precision có thể thấp. Trong production, false positive cao sẽ ảnh hưởng trực tiếp đến trải nghiệm người dùng và làm tăng khối lượng điều tra. Cách xử lý là tune threshold, phân tầng ALERT và BLOCK rõ hơn, calibration model và đánh giá lại trên dữ liệu thật.

Về hạ tầng, ClickHouse sink hiện là near exactly-once chứ chưa phải exactly-once tuyệt đối. Kafka trong demo có thể là single broker, còn production nên dùng cluster 3 broker trở lên.

State backend hiện tại nếu dùng HashMap thì phù hợp demo, nhưng khi state lớn nên chuyển sang RocksDB. Ngoài ra, model chưa có reload động, nên hướng nâng cấp là thêm model registry hoặc versioning.

Khi bị hỏi phản biện, em sẽ nhấn mạnh rằng project tập trung chứng minh kiến trúc realtime end-to-end, đồng thời đã nhận diện rõ các điểm cần hardening nếu đưa vào production.

Câu chuyển ý:

Cuối cùng, em sẽ kết luận lại những gì hệ thống đã đạt được và hướng phát triển tiếp theo.

## Slide 28: Kết luận và hướng phát triển

Script:

Tóm lại, project đã xây dựng được một pipeline realtime fraud detection end-to-end, bắt đầu từ data generation, đi qua Kafka, xử lý bằng Flink, lưu vào ClickHouse và hiển thị trên Grafana.

Phần xử lý chính dùng Flink với event time, watermark, checkpoint và state theo account. Đây là nền tảng quan trọng để xử lý stream một cách ổn định và có ngữ cảnh.

Về phát hiện gian lận, hệ thống kết hợp ba lớp: Rules Engine cho điều kiện rõ ràng, CEP Engine cho chuỗi hành vi và ML Scoring cho đánh giá xác suất rủi ro. Decision Engine tổng hợp các tín hiệu này để đưa ra APPROVE, ALERT hoặc BLOCK.

Hệ thống cũng có khả năng quan sát qua dashboard, giúp theo dõi throughput, latency, decision breakdown và các metric như FDR, FPR, Precision.

Nếu phát triển tiếp, em sẽ ưu tiên giảm false positive và tăng precision, bổ sung confusion matrix, Precision/Recall/F1 rõ hơn, thêm model versioning hoặc reload động, triển khai Kafka và ClickHouse dạng cluster, đồng thời bổ sung security và monitoring đầy đủ hơn.

Câu kết:

Trên đây là phần trình bày của em về Real-Time Fraud Detection Platform. Em xin cảm ơn thầy cô và các bạn đã lắng nghe, và em sẵn sàng trả lời câu hỏi.

## Kịch bản nói nhanh 1 phút cuối

Script:

Nếu cần tóm tắt toàn bộ hệ thống trong khoảng 1 phút, em có thể trình bày như sau:

Hệ thống của em là một nền tảng phát hiện gian lận giao dịch theo thời gian thực. Data Simulator sinh transaction và rule update rồi gửi vào Kafka. Flink đọc stream giao dịch, xử lý theo event time với watermark 5 giây, sau đó đưa dữ liệu qua ba lớp phát hiện gồm Rules Engine, CEP Engine và ML Scoring.

Rules Engine dùng Broadcast State để cập nhật luật động. CEP Engine phát hiện các chuỗi hành vi như account takeover, micro transactions, velocity attack và impossible travel. ML Scoring dùng model Random Forest export ONNX và chạy trực tiếp trong JVM.

Decision Engine tổng hợp các tín hiệu để xuất APPROVE, ALERT hoặc BLOCK. Kết quả được ghi vào ClickHouse và hiển thị trên Grafana. Hệ thống có checkpoint, state theo account, dashboard latency và throughput, đồng thời có thể mở rộng alert sang SMS, email, case management hoặc core banking.

## Gợi ý trả lời câu hỏi phản biện

### Vì sao không chỉ dùng Machine Learning?

Không chỉ dùng ML vì fraud detection cần cả độ chính xác và khả năng giải thích. Rule giúp bắt các trường hợp rõ ràng như blacklist hoặc amount vượt ngưỡng. CEP giúp phát hiện chuỗi hành vi mà một event đơn lẻ không thể hiện hết. ML bổ sung khả năng đánh giá xác suất dựa trên nhiều feature. Kết hợp ba lớp giúp hệ thống linh hoạt và dễ giải thích hơn.

### Vì sao dùng Flink thay vì xử lý bằng service thông thường?

Flink phù hợp vì đây là bài toán stream processing có state và event time. Hệ thống cần xử lý dữ liệu liên tục, lưu trạng thái theo account, dùng watermark, checkpoint và CEP. Nếu dùng service thông thường, mình sẽ phải tự xây dựng nhiều cơ chế mà Flink đã hỗ trợ sẵn.

### Vì sao dùng Kafka?

Kafka giúp tách producer và consumer, buffer dữ liệu, hỗ trợ partition để mở rộng throughput và cho phép replay event khi cần. Với hệ thống realtime, Kafka là lớp trung gian ổn định giữa simulator, Flink và các consumer khác.

### Exactly-once của hệ thống đang ở mức nào?

Kafka source và Flink state có thể đạt exactly-once theo checkpoint. Tuy nhiên, ClickHouse custom sink chưa phải TwoPhaseCommit sink, nên end-to-end tới ClickHouse nên gọi là near exactly-once. Hệ thống giảm trùng lặp bằng retry, checkpoint buffer và ReplacingMergeTree để dedup.

### Nếu đưa vào production cần cải tiến gì trước?

Các ưu tiên gồm dùng dữ liệu thật để validate, tune threshold để giảm false positive, thêm model registry hoặc model versioning, dùng Kafka cluster nhiều broker, chuyển state backend phù hợp khi state lớn, tăng monitoring và bổ sung cơ chế sink idempotent hoặc transactional chặt hơn.
