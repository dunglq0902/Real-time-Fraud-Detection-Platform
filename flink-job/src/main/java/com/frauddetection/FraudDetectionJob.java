package com.frauddetection;

import com.frauddetection.cep.CepAlertMerger;
import com.frauddetection.cep.FraudPatternDetector;
import com.frauddetection.decision.DecisionMaker;
import com.frauddetection.features.FeatureAggregator;
import com.frauddetection.ml.OnnxModelScorer;
import com.frauddetection.model.DecisionResult;
import com.frauddetection.model.FraudAlert;
import com.frauddetection.model.Transaction;
import com.frauddetection.rules.RulesEngine;
import com.frauddetection.serialization.FraudAlertSerializer;
import com.frauddetection.serialization.FraudRuleDeserializer;
import com.frauddetection.serialization.TransactionDeserializer;
import com.frauddetection.sink.AlertClickHouseSink;
import com.frauddetection.sink.ClickHouseSink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Real-Time Fraud Detection Pipeline — Apache Flink (Java).
 *
 * <p>
 * Orchestrates the full streaming pipeline:
 * <ol>
 * <li>Kafka Source → Parse & Filter Transactions</li>
 * <li>Adaptive Rules Engine — Broadcast State Pattern (HIGHEST PRIORITY)</li>
 * <li>CEP Pattern Detection — 3 patterns via Flink CEP API (MEDIUM
 * PRIORITY)</li>
 * <li>Merge CEP alerts into main stream</li>
 * <li>Feature Aggregator — Keyed State with TTL (short-circuits CRITICAL)</li>
 * <li>ML Model Scoring — ONNX Runtime in-memory (short-circuits CRITICAL)</li>
 * <li>Decision Engine (Rules &gt; CEP &gt; ML)</li>
 * <li>Sinks: ClickHouse + Kafka alerts</li>
 * </ol>
 */
public class FraudDetectionJob {

        private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionJob.class);

        // ── Configuration ─────────────────────────────────────
        private static final String KAFKA_BOOTSTRAP = System.getenv().getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        private static final String MINIO_ENDPOINT = System.getenv().getOrDefault(
                        "MINIO_ENDPOINT", "minio:9000");
        private static final String MINIO_ACCESS_KEY = System.getenv().getOrDefault(
                        "MINIO_ACCESS_KEY", "admin");
        private static final String MINIO_SECRET_KEY = System.getenv().getOrDefault(
                        "MINIO_SECRET_KEY", "password123");
        private static final String CLICKHOUSE_HOST = System.getenv().getOrDefault(
                        "CLICKHOUSE_HOST", "clickhouse");
        private static final int CLICKHOUSE_PORT = Integer.parseInt(
                        System.getenv().getOrDefault("CLICKHOUSE_PORT", "8123"));
        private static final String CLICKHOUSE_USER = System.getenv().getOrDefault(
                        "CLICKHOUSE_USER", "default");
        private static final String CLICKHOUSE_PASSWORD = System.getenv().getOrDefault(
                        "CLICKHOUSE_PASSWORD", "clickhouse123");

        public static void main(String[] args) throws Exception {
                LOG.info("======================================================================");
                LOG.info("  REAL-TIME FRAUD DETECTION PIPELINE — Java Flink");
                LOG.info("  CEP: Flink CEP API (3 patterns)");
                LOG.info("  Rules: Broadcast State Pattern");
                LOG.info("  ML: ONNX Runtime (In-memory)");
                LOG.info("======================================================================");

                // ═══════════════════════════════════════════════════
                // 1. Environment Setup
                // ═══════════════════════════════════════════════════
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(8);

                // Checkpointing: 60s interval, EXACTLY_ONCE mode
                env.enableCheckpointing(60000);
                CheckpointConfig checkpointConfig = env.getCheckpointConfig();
                checkpointConfig.setCheckpointingMode(
                                org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
                checkpointConfig.setMinPauseBetweenCheckpoints(30000); // 30s
                checkpointConfig.setCheckpointTimeout(120000); // 120s
                checkpointConfig.setMaxConcurrentCheckpoints(1);
                // Unaligned checkpoints: barriers pass through without alignment,
                // so processing is never paused waiting for barrier alignment.
                // This is critical for avoiding the checkpoint stall that caused >2min latency.
                checkpointConfig.enableUnalignedCheckpoints();
                // Retain checkpoints on cancellation for recovery
                checkpointConfig.setExternalizedCheckpointCleanup(
                                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

                // ═══════════════════════════════════════════════════
                // 2. Kafka Sources
                // ═══════════════════════════════════════════════════
                // Transaction source with event time watermarks
                // Bounded out-of-orderness = 5 seconds (per specification)
                KafkaSource<Transaction> txSource = KafkaSource.<Transaction>builder()
                                .setBootstrapServers(KAFKA_BOOTSTRAP)
                                .setTopics("transactions")
                                .setGroupId("fraud-detection-group")
                                .setStartingOffsets(OffsetsInitializer.latest())
                                .setDeserializer(KafkaRecordDeserializationSchema
                                                .valueOnly(new TransactionDeserializer()))
                                .build();

                WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
                                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((tx, ts) -> tx.getTimestamp())
                                .withIdleness(Duration.ofSeconds(30));

                DataStream<Transaction> txStream = env
                                .fromSource(txSource, watermarkStrategy, "KafkaTransactionSource")
                                .filter(tx -> tx != null && tx.getTransactionId() != null)
                                .name("FilterInvalidTransactions");

                // Rules source (broadcast stream)
                KafkaSource<String> rulesSource = KafkaSource.<String>builder()
                                .setBootstrapServers(KAFKA_BOOTSTRAP)
                                .setTopics("fraud-rules")
                                .setGroupId("fraud-rules-group")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setDeserializer(
                                                KafkaRecordDeserializationSchema.valueOnly(new FraudRuleDeserializer()))
                                .build();

                WatermarkStrategy<String> rulesWatermarkStrategy = ctx -> new WatermarkGenerator<String>() {
                        @Override
                        public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
                                output.emitWatermark(new Watermark(Long.MAX_VALUE - 1000));
                        }

                        @Override
                        public void onPeriodicEmit(WatermarkOutput output) {
                                output.emitWatermark(new Watermark(Long.MAX_VALUE - 1000));
                        }
                };

                DataStream<String> rulesStream = env
                                .fromSource(rulesSource, rulesWatermarkStrategy, "KafkaRulesSource");

                // ═══════════════════════════════════════════════════
                // 3. Adaptive Rules Engine (Broadcast State) — HIGHEST PRIORITY
                // Rules are fast, stateless checks (blacklist, threshold, etc.)
                // Run FIRST to flag/block obvious fraud before heavier processing.
                // ═══════════════════════════════════════════════════
                MapStateDescriptor<String, String> rulesStateDesc = new MapStateDescriptor<>(
                                "fraud-rules",
                                Types.STRING,
                                Types.STRING);
                BroadcastStream<String> broadcastRules = rulesStream.broadcast(rulesStateDesc);

                DataStream<Transaction> rulesEvaluated = txStream
                                .keyBy(Transaction::getAccountId)
                                .connect(broadcastRules)
                                .process(new RulesEngine(rulesStateDesc))
                                .name("RulesEngine");

                // ═══════════════════════════════════════════════════
                // 4. CEP Pattern Detection — MEDIUM PRIORITY
                // Stateful pattern matching (ATO, Smurfing, Impossible Travel).
                // Runs on the rules-enriched stream so CEP results are additive.
                // ═══════════════════════════════════════════════════
                KeyedStream<Transaction, String> keyedRulesStream = rulesEvaluated
                                .keyBy(Transaction::getAccountId);

                // Apply all 3 CEP patterns → unified alert stream
                FraudPatternDetector cepDetector = new FraudPatternDetector();
                DataStream<FraudAlert> cepAlerts = cepDetector.detectAll(keyedRulesStream);

                // ═══════════════════════════════════════════════════
                // 5. Merge CEP Alerts into Main Stream
                // Synchronizes CEP alerts with the rules-enriched transactions.
                // ═══════════════════════════════════════════════════
                // Reuse keyedRulesStream (already keyed by accountId in step 4)
                // instead of calling rulesEvaluated.keyBy() again — eliminates one
                // redundant network shuffle per event, reducing serialization overhead.
                DataStream<Transaction> cepEnrichedStream = keyedRulesStream
                                .connect(cepAlerts.keyBy(FraudAlert::getAccountId))
                                .process(new CepAlertMerger())
                                .name("CepAlertMerger");

                // ═══════════════════════════════════════════════════
                // 6. Feature Aggregator (Keyed State + TTL)
                // Short-circuits CRITICAL transactions (skips feature computation).
                // ═══════════════════════════════════════════════════
                DataStream<Transaction> featuresComputed = cepEnrichedStream
                                .keyBy(Transaction::getAccountId)
                                .process(new FeatureAggregator())
                                .name("FeatureAggregator");

                // ═══════════════════════════════════════════════════
                // 7. ML Model Scoring (ONNX Runtime in-memory) — LOWEST PRIORITY
                // Short-circuits CRITICAL transactions (skips ML inference).
                // ═══════════════════════════════════════════════════
                DataStream<Transaction> mlScored = featuresComputed
                                .flatMap(new OnnxModelScorer(
                                                MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
                                                "ml-models", "fraud_detector.onnx"))
                                .name("OnnxModelScorer");

                // ═══════════════════════════════════════════════════
                // 8. Decision Engine (Rules > CEP > ML)
                // ═══════════════════════════════════════════════════
                DataStream<DecisionResult> decisions = mlScored
                                .flatMap(new DecisionMaker())
                                .name("DecisionMaker");

                // ═══════════════════════════════════════════════════
                // 9. Sinks
                // ═══════════════════════════════════════════════════

                // 9a: Decisions → ClickHouse (transactions table)
                // Batch size 500 (was 2000) and flush interval 2000ms (was 5000ms)
                // to reduce end-to-end latency visible in Grafana. At 500 events/s,
                // batch fills in ~1s; flush timer acts as 2s safety net.
                decisions.addSink(new ClickHouseSink(
                                "fraud_detection.transactions", 500, 2000,
                                CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD))
                                .name("ClickHouseTransactionSink");

                // 9b: Final Decision alerts → ClickHouse (fraud_alerts table)
                // Extract only final ALERT/BLOCK outcomes after DecisionMaker has applied
                // priority.
                DataStream<FraudAlert> allAlerts = decisions
                                .flatMap((DecisionResult decision,
                                                org.apache.flink.util.Collector<FraudAlert> collector) -> {
                                        if (isActionableDecision(decision)) {
                                                collector.collect(toFinalFraudAlert(decision));
                                        }
                                })
                                .returns(FraudAlert.class)
                                .name("AlertExtractor");

                allAlerts.addSink(new AlertClickHouseSink(
                                CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD))
                                .name("ClickHouseAlertSink");

                // 9c: Alerts → Kafka fraud-alerts topic
                KafkaSink<FraudAlert> kafkaAlertSink = KafkaSink.<FraudAlert>builder()
                                .setBootstrapServers(KAFKA_BOOTSTRAP)
                                .setRecordSerializer(
                                                KafkaRecordSerializationSchema.builder()
                                                                .setTopic("fraud-alerts")
                                                                .setValueSerializationSchema(new FraudAlertSerializer())
                                                                .build())
                                .build();
                allAlerts.sinkTo(kafkaAlertSink).name("KafkaAlertSink");

                // ═══════════════════════════════════════════════════
                // Execute
                // ═══════════════════════════════════════════════════
                LOG.info("Starting Flink Fraud Detection Job...");
                env.execute("RealTime-Fraud-Detection-Pipeline");
        }

        private static boolean isActionableDecision(DecisionResult decision) {
                return decision != null
                                && ("ALERT".equals(decision.getDecision()) || "BLOCK".equals(decision.getDecision()));
        }

        private static FraudAlert toFinalFraudAlert(DecisionResult decision) {
                String finalDecision = decision.getDecision();
                String decisionSource = nonBlankOrDefault(decision.getDecisionSource(), "DECISION_ENGINE");
                String alertSource = "CEP_ENGINE".equals(decisionSource) ? "CEP" : decisionSource;
                String severity = "BLOCK".equals(finalDecision) ? "CRITICAL" : "HIGH";
                String triggeredRules = nonBlankOrDefault(decision.getRuleTriggered(), "none");

                String details = String.format(
                                "Final decision=%s, source=%s, combined_score=%.4f, ml_score=%.4f, triggered_rules=%s",
                                finalDecision,
                                decisionSource,
                                decision.getCombinedScore(),
                                decision.getMlScore(),
                                triggeredRules);

                FraudAlert alert = new FraudAlert(
                                decision.getTransactionId(),
                                decision.getAccountId(),
                                "FINAL_" + finalDecision,
                                alertSource,
                                severity,
                                decision.getCombinedScore(),
                                details,
                                "Decision Engine priority: Rules > CEP > ML",
                                decision.getEventTime());
                alert.setAlertId(buildFinalAlertId(decision));
                return alert;
        }

        private static String buildFinalAlertId(DecisionResult decision) {
                return "FINAL_" + nonBlankOrDefault(decision.getTransactionId(), "UNKNOWN");
        }

        private static String nonBlankOrDefault(String value, String defaultValue) {
                return value != null && !value.isBlank() ? value : defaultValue;
        }
}
