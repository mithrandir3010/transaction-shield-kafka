# ── Fraud Engine — Runtime Image ─────────────────────────────────────────────
# Build the JAR first:  mvn package -pl fraud-engine -am -DskipTests
# Build the image:      docker build -t ts-fraud-engine fraud-engine/
# Run locally:          docker run -p 8083:8083 --env-file .env ts-fraud-engine
# ─────────────────────────────────────────────────────────────────────────────

FROM eclipse-temurin:21-jre-alpine

ARG BUILD_DATE
ARG GIT_SHA

LABEL org.opencontainers.image.title="Transaction Shield — Fraud Engine"
LABEL org.opencontainers.image.description="Kafka consumer that applies Rule Engine and scores transactions"
LABEL org.opencontainers.image.source="https://github.com/mithrandir3010/transaction-shield-kafka"
LABEL org.opencontainers.image.created="${BUILD_DATE}"
LABEL org.opencontainers.image.revision="${GIT_SHA}"

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app
COPY target/*.jar app.jar
RUN chown appuser:appgroup app.jar

USER appuser

EXPOSE 8083

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD wget -qO- http://localhost:8083/actuator/health | grep -q '"status":"UP"' || exit 1

ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-XX:+ExitOnOutOfMemoryError", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-Dspring.profiles.active=${SPRING_PROFILES_ACTIVE:}", \
    "-jar", "app.jar"]
