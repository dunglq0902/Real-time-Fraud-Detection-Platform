"""
=============================================================
ML Model Trainer for Fraud Detection
=============================================================
Trains a Random Forest classifier on synthetic fraud data,
exports to ONNX format, and uploads to MinIO.
=============================================================
"""

import os
import logging
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, average_precision_score
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import onnxruntime as ort

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MLTrainer")

MODEL_NAME = os.getenv("MODEL_NAME", "fraud_detector.onnx")
OUTPUT_PATH = f"/app/{MODEL_NAME}"

# Feature columns (must match what Flink sends):
# [amount, hour_of_day, is_foreign, tx_frequency_1h, distance_km]
FEATURE_NAMES = ["amount", "hour_of_day", "is_foreign", "tx_frequency_1h", "distance_km"]
NUM_FEATURES = len(FEATURE_NAMES)


def generate_synthetic_data(n_samples=50000):
    """
    Generate synthetic fraud detection training data.
    Features: amount, hour_of_day, is_foreign, tx_frequency_1h, distance_km
    """
    logger.info(f"Generating {n_samples} synthetic samples...")
    np.random.seed(42)

    # ── Legitimate transactions (97%) ──
    n_legit = int(n_samples * 0.97)
    legit_amount = np.random.lognormal(mean=5.5, sigma=1.0, size=n_legit)
    legit_amount = np.clip(legit_amount, 1, 10000)
    legit_hour = np.random.choice(range(6, 23), size=n_legit)
    legit_foreign = np.random.binomial(1, 0.05, size=n_legit)
    legit_freq = np.random.poisson(2, size=n_legit)
    legit_distance = np.random.exponential(10, size=n_legit)
    legit_distance = np.clip(legit_distance, 0, 100)

    legit_features = np.column_stack([
        legit_amount, legit_hour, legit_foreign, legit_freq, legit_distance
    ])
    legit_labels = np.zeros(n_legit)

    # ── Fraudulent transactions (3%) ──
    n_fraud = n_samples - n_legit
    fraud_amount = np.random.lognormal(mean=8.5, sigma=1.5, size=n_fraud)
    fraud_amount = np.clip(fraud_amount, 500, 100000)
    fraud_hour = np.random.choice(list(range(0, 6)) + [23], size=n_fraud)
    fraud_foreign = np.random.binomial(1, 0.7, size=n_fraud)
    fraud_freq = np.random.poisson(8, size=n_fraud)
    fraud_distance = np.random.exponential(500, size=n_fraud)
    fraud_distance = np.clip(fraud_distance, 50, 20000)

    fraud_features = np.column_stack([
        fraud_amount, fraud_hour, fraud_foreign, fraud_freq, fraud_distance
    ])
    fraud_labels = np.ones(n_fraud)

    # ── Combine ──
    X = np.vstack([legit_features, fraud_features]).astype(np.float32)
    y = np.concatenate([legit_labels, fraud_labels]).astype(np.int64)

    # Shuffle
    indices = np.random.permutation(len(X))
    X, y = X[indices], y[indices]

    logger.info(f"Dataset: {n_legit} legit + {n_fraud} fraud = {n_samples} total")
    return X, y


def train_model(X, y):
    """Train Random Forest classifier."""
    logger.info("Splitting data into train/test sets...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    logger.info("Training Random Forest classifier...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        min_samples_split=5,
        min_samples_leaf=2,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    logger.info("\n" + classification_report(y_test, y_pred, target_names=["Legit", "Fraud"]))

    # AUC metrics (required by specification)
    auc_roc = roc_auc_score(y_test, y_prob)
    auc_pr = average_precision_score(y_test, y_prob)
    logger.info(f"AUC-ROC: {auc_roc:.4f} (required > 0.85) {'✅ PASS' if auc_roc > 0.85 else '❌ FAIL'}")
    logger.info(f"AUC-PR:  {auc_pr:.4f} (required > 0.70) {'✅ PASS' if auc_pr > 0.70 else '❌ FAIL'}")

    return model


def export_to_onnx(model, output_path):
    """Convert sklearn model to ONNX format."""
    logger.info(f"Exporting model to ONNX: {output_path}")

    initial_type = [("float_input", FloatTensorType([None, NUM_FEATURES]))]
    onnx_model = convert_sklearn(
        model,
        initial_types=initial_type,
        target_opset=12,
        options={id(model): {"zipmap": False}},  # Output probabilities as array
    )

    with open(output_path, "wb") as f:
        f.write(onnx_model.SerializeToString())

    # Verify ONNX model
    logger.info("Verifying ONNX model...")
    session = ort.InferenceSession(output_path)
    test_input = np.random.randn(1, NUM_FEATURES).astype(np.float32)
    input_name = session.get_inputs()[0].name
    results = session.run(None, {input_name: test_input})
    logger.info(f"ONNX verification passed. Output shape: {[r.shape for r in results]}")

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info(f"Model file size: {file_size_mb:.2f} MB")

    return output_path


def upload_to_minio(file_path):
    """Upload ONNX model file to MinIO."""
    from minio import Minio

    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password123")
    bucket = os.getenv("MINIO_BUCKET", "ml-models")
    model_name = os.getenv("MODEL_NAME", "fraud_detector.onnx")

    logger.info(f"Uploading model to MinIO: s3://{bucket}/{model_name}")

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    # Ensure bucket exists
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info(f"Created bucket: {bucket}")

    # Upload
    client.fput_object(
        bucket,
        model_name,
        file_path,
        content_type="application/octet-stream",
    )

    # Verify
    stat = client.stat_object(bucket, model_name)
    logger.info(f"Upload successful! Size: {stat.size} bytes, ETag: {stat.etag}")


def main():
    logger.info("=" * 60)
    logger.info("  FRAUD DETECTION ML MODEL TRAINER")
    logger.info("=" * 60)

    # Step 1: Generate data
    X, y = generate_synthetic_data(n_samples=50000)

    # Step 2: Train model
    model = train_model(X, y)

    # Step 3: Export to ONNX
    onnx_path = export_to_onnx(model, OUTPUT_PATH)

    # Step 4: Upload to MinIO
    upload_to_minio(onnx_path)

    logger.info("=" * 60)
    logger.info("  MODEL TRAINING AND UPLOAD COMPLETE!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
