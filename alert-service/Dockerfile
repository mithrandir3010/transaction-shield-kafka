# ── Alert Service — Runtime Image ────────────────────────────────────────────
# Build the JAR first:  mvn package -pl alert-service -am -DskipTests
# Build the image:      docker build -t ts-alert-service alert-service/
# Run locally:          docker run -p 8084:8084 --env-file .env ts-alert-service
# ─────────────────────────────────────────────────────────────────────────────

FROM eclipse-temurin:21-jre-alpine

ARG BUILD_DATE
ARG GIT_SHA

LABEL org.opencontainers.image.title="Transaction Shield — Alert Service"
LABEL org.opencontainers.image.description="Consumes scored transactions, persists alerts, notifies downstream"
LABEL org.opencontainers.image.source="https://github.com/mithrandir3010/transaction-shield-kafka"
LABEL org.opencontainers.image.created="${BUILD_DATE}"
LABEL org.opencontainers.image.revision="${GIT_SHA}"

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app
COPY target/*.jar app.jar
RUN chown appuser:appgroup app.jar

USER appuser

EXPOSE 8084

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD wget -qO- http://localhost:8084/actuator/health | grep -q '"status":"UP"' || exit 1

ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-XX:+ExitOnOutOfMemoryError", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-Dspring.profiles.active=${SPRING_PROFILES_ACTIVE:}", \
    "-jar", "app.jar"]
