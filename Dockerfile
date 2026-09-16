# --- Build stage: compile and package with Gradle, cached separately from the runtime image ---
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace

# Copy the wrapper and build files first so dependency resolution is cached
# across builds where only application source changes.
COPY gradlew ./
COPY gradle ./gradle
COPY settings.gradle build.gradle ./
RUN chmod +x gradlew && ./gradlew --version

COPY src ./src
RUN ./gradlew installDist --no-daemon

# --- Runtime stage: small JRE-only image with just the built application ---
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

RUN useradd --system --create-home --shell /usr/sbin/nologin appuser
COPY --from=build /workspace/build/install/distributed-priority-queue-service ./
RUN chown -R appuser:appuser /app
USER appuser

ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["./bin/distributed-priority-queue-service"]
