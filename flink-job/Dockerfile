# ═══════════════════════════════════════════════════════
# Stage 1: Maven Build — Compile Java Flink job into fat JAR
# ═══════════════════════════════════════════════════════
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build
COPY pom.xml .
# Download dependencies first (leverages Docker layer cache)
RUN mvn dependency:go-offline -B

COPY src/ src/
RUN mvn clean package -DskipTests -q

# ═══════════════════════════════════════════════════════
# Stage 2: Flink Runtime — Lean production image
# ═══════════════════════════════════════════════════════
FROM flink:1.18.1-java11

# Copy the fat JAR from build stage
RUN mkdir -p /opt/flink/jobs
COPY --from=builder /build/target/fraud-detection-job-1.0.0.jar /opt/flink/jobs/

# Enable S3 plugin for MinIO (checkpoints and savepoints)
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/
