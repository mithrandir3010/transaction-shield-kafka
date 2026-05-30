# ── Transaction Producer — Runtime Image ─────────────────────────────────────
# Build the JAR first:  mvn package -pl transaction-producer -am -DskipTests
# Build the image:      docker build -t ts-transaction-producer transaction-producer/
# Run locally:          docker run -p 8082:8082 --env-file .env ts-transaction-producer
# ─────────────────────────────────────────────────────────────────────────────

FROM eclipse-temurin:21-jre-alpine

ARG BUILD_DATE
ARG GIT_SHA

LABEL org.opencontainers.image.title="Transaction Shield — Transaction Producer"
LABEL org.opencontainers.image.description="REST API that accepts transactions and publishes to Kafka"
LABEL org.opencontainers.image.source="https://github.com/mithrandir3010/transaction-shield-kafka"
LABEL org.opencontainers.image.created="${BUILD_DATE}"
LABEL org.opencontainers.image.revision="${GIT_SHA}"

# Non-root user (CIS benchmark requirement)
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app
COPY target/*.jar app.jar
RUN chown appuser:appgroup app.jar

USER appuser

EXPOSE 8082

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD wget -qO- http://localhost:8082/actuator/health | grep -q '"status":"UP"' || exit 1

ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-XX:+ExitOnOutOfMemoryError", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-Dspring.profiles.active=${SPRING_PROFILES_ACTIVE:}", \
    "-jar", "app.jar"]
