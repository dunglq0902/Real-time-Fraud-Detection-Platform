package com.frauddetection.ml;

import com.frauddetection.model.AggregateFeatures;
import com.frauddetection.model.Transaction;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.OrtException;

import io.minio.MinioClient;
import io.minio.GetObjectArgs;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.UUID;

/**
 * ONNX Model Scorer — loads the ML model from MinIO once in {@code open()},
 * then scores every transaction in-memory using ONNX Runtime (Java).
 *
 * <p>
 * <b>In-Memory Inference:</b> ONNX Runtime runs inference directly on the JVM
 * with zero network overhead — nanosecond-level latency per prediction.
 * The model is downloaded from MinIO only once during operator initialization.
 *
 * <p>
 * Feature vector (matching train_model.py):
 * {@code [amount, hour_of_day, is_foreign, tx_frequency_1h, distance_km]}
 */
public class OnnxModelScorer extends RichFlatMapFunction<Transaction, Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OnnxModelScorer.class);

    private final String minioEndpoint;
    private final String minioAccessKey;
    private final String minioSecretKey;
    private final String minioBucket;
    private final String modelName;

    private transient OrtEnvironment ortEnv;
    private transient OrtSession ortSession;
    private transient String inputName;

    /**
     * Pre-allocated input buffer — reused for every inference call to avoid GC
     * pressure.
     */
    private transient float[][] inputBuffer;

    public OnnxModelScorer(String minioEndpoint, String minioAccessKey,
            String minioSecretKey, String minioBucket, String modelName) {
        this.minioEndpoint = minioEndpoint;
        this.minioAccessKey = minioAccessKey;
        this.minioSecretKey = minioSecretKey;
        this.minioBucket = minioBucket;
        this.modelName = modelName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Loading ONNX model from MinIO: s3://{}/{}", minioBucket, modelName);

        try {
            // Download model from MinIO
            String endpoint = minioEndpoint;
            if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
                endpoint = "http://" + endpoint;
            }
            MinioClient client = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(minioAccessKey, minioSecretKey)
                    .build();

            String tempPath = System.getProperty("java.io.tmpdir") + "/fraud_detector_"
                    + UUID.randomUUID().toString().substring(0, 8) + ".onnx";
            File tempFile = new File(tempPath);

            try (InputStream is = client.getObject(
                    GetObjectArgs.builder()
                            .bucket(minioBucket)
                            .object(modelName)
                            .build());
                    FileOutputStream fos = new FileOutputStream(tempFile)) {

                byte[] buf = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buf)) != -1) {
                    fos.write(buf, 0, bytesRead);
                }
            }

            LOG.info("Model downloaded to {}", tempPath);

            // Initialize ONNX Runtime session
            ortEnv = OrtEnvironment.getEnvironment();
            OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions();
            sessionOptions.setIntraOpNumThreads(1);
            sessionOptions.setInterOpNumThreads(1);
            sessionOptions.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);

            ortSession = ortEnv.createSession(tempPath, sessionOptions);
            inputName = ortSession.getInputNames().iterator().next();
            inputBuffer = new float[1][5]; // [amount, hour_of_day, is_foreign, tx_frequency_1h, distance_km]

            LOG.info("ONNX model loaded successfully! Input name: {}", inputName);

            // Cleanup temp file immediately since it's already loaded into memory
            if (tempFile.exists()) {
                tempFile.delete();
            }

        } catch (Exception e) {
            LOG.error("Failed to load ONNX model: {}. ML scoring will be disabled.", e.getMessage());
            ortSession = null;
        }
    }

    @Override
    public void flatMap(Transaction tx, Collector<Transaction> out) throws Exception {
        try {
            // Bypass late alert transactions
            if (tx.isLateAlert()) {
                tx.setMlScore(0.0);
                tx.setMlAvailable(false);
                out.collect(tx);
                return;
            }

            // ── Short-circuit: skip ML inference for CRITICAL fraud ──
            // If Rules or CEP already flagged this transaction as CRITICAL,
            // ML scoring is unnecessary (Decision Engine would ignore ML anyway).
            // This saves expensive ONNX tensor allocation + inference per transaction.
            if (tx.hasCriticalAlert()) {
                LOG.debug("Short-circuit OnnxModelScorer for CRITICAL tx {} (account {})",
                        tx.getTransactionId(), tx.getAccountId());
                tx.setMlScore(0.0);
                tx.setMlAvailable(false);
                out.collect(tx);
                return;
            }

            if (ortSession == null) {
                tx.setMlScore(0.0);
                tx.setMlAvailable(false);
                out.collect(tx);
                return;
            }

            // Extract feature vector into pre-allocated buffer (zero allocation)
            extractFeaturesInto(tx, inputBuffer[0]);

            OnnxTensor inputTensor = null;
            var results = (ai.onnxruntime.OrtSession.Result) null;
            try {
                inputTensor = OnnxTensor.createTensor(ortEnv, inputBuffer);
                results = ortSession.run(Collections.singletonMap(inputName, inputTensor));

                // Extract fraud probability from model output
                double fraudProb = 0.0;
                // results[1] contains probabilities: [[prob_legit, prob_fraud]]
                if (results.size() > 1) {
                    Object probResult = results.get(1).getValue();
                    if (probResult instanceof float[][]) {
                        float[][] probs = (float[][]) probResult;
                        fraudProb = probs[0].length > 1 ? probs[0][1] : probs[0][0];
                    }
                } else {
                    // Fall back to label output
                    Object labelResult = results.get(0).getValue();
                    if (labelResult instanceof long[]) {
                        fraudProb = ((long[]) labelResult)[0];
                    }
                }

                tx.setMlScore(Math.round(fraudProb * 1_000_000.0) / 1_000_000.0);
                tx.setMlAvailable(true);
            } finally {
                if (results != null)
                    results.close();
                if (inputTensor != null)
                    inputTensor.close();
            }

            out.collect(tx);

        } catch (Exception e) {
            LOG.error("ML scoring error for tx {}: {}", tx.getTransactionId(), e.getMessage());
            tx.setMlScore(0.0);
            tx.setMlAvailable(false);
            out.collect(tx);
        }
    }

    /**
     * Extract features directly into the pre-allocated buffer (zero allocation).
     * Features: [amount, hour_of_day, is_foreign, tx_frequency_1h, distance_km]
     */
    private void extractFeaturesInto(Transaction tx, float[] buf) {
        buf[0] = (float) tx.getAmount();

        // Hour of day (local Asia/Ho_Chi_Minh)
        if (tx.getTimestamp() > 0) {
            buf[1] = (float) Instant.ofEpochMilli(tx.getTimestamp())
                    .atZone(java.time.ZoneId.of("Asia/Ho_Chi_Minh"))
                    .getHour();
        } else {
            buf[1] = 12.0f; // default
        }

        // Is foreign (outside Vietnam approximate bounding box)
        double lat = tx.getLatitude();
        double lon = tx.getLongitude();
        buf[2] = (lat >= 8.0 && lat <= 23.5 && lon >= 102.0 && lon <= 110.0) ? 0.0f : 1.0f;

        // Transaction frequency from aggregate features
        AggregateFeatures agg = tx.getAggFeatures();
        buf[3] = (agg != null) ? (float) agg.getTxCount1h() : 1.0f;

        // Distance from last location (calculated from features)
        buf[4] = (agg != null) ? (float) agg.getDistanceKm() : 0.0f;
    }

    @Override
    public void close() throws Exception {
        if (ortSession != null) {
            LOG.info("Closing ONNX session...");
            ortSession.close();
            ortSession = null;
        }
        // Do NOT close ortEnv because it is a shared singleton for the JVM.
        // Closing it will cause other parallel subtasks in the same TaskManager to crash.
        ortEnv = null;
    }
}
