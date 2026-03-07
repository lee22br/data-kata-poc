#!/usr/bin/env bash
# Compiles the fat JAR inside a Maven Docker container.
# Output: flink-job/target/flink-job-1.0.jar
set -euo pipefail
cd "$(dirname "$0")"

echo "Building fat JAR..."
docker run --rm \
  -v "$(pwd)/flink-job":/app \
  -v "$HOME/.m2":/root/.m2 \
  -w /app \
  maven:3.9-eclipse-temurin-11 \
  mvn package -DskipTests -q

echo "JAR ready: flink-job/target/flink-job-1.0.jar"
