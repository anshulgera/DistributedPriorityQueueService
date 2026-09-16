# Distributed Priority Queue Service

**Status: walking skeleton.** This step contains no queue logic — it exists only
to prove the build/test/deploy/promote pipeline works end-to-end against a
trivial service. A single `GET /health` endpoint is the entire API surface.

## Why Javalin

The service needed a lightweight embedded HTTP layer for a single endpoint,
not a framework. Javalin gives an embedded Jetty server with no
classpath-scanning autoconfiguration, a handful of dependencies, and
sub-second startup — which matters for a service that will later need tight
control over its own networking and threading model. Spring Boot's
component-scanning and auto-configuration machinery would add startup latency
and "magic" that isn't earning its keep for either this trivial skeleton or
the low-level control a priority queue implementation will eventually want.

## Project layout

- `src/main` — the service (`App`, `HealthStatus`)
- `src/test` — unit tests (JUnit 5), run by `./gradlew test`
- `src/integrationTest` — a dedicated source set for smoke/integration tests
  (JUnit 5 + REST Assured) that run over real HTTP against a **running,
  deployed instance**, not in-process. Run by `./gradlew integrationTest`.
  These two source sets have independent Gradle tasks so they can be gated as
  separate CI pipeline stages.

## Running locally

```bash
# Unit tests only
./gradlew test

# Run the service (defaults to :8080; override with PORT)
./gradlew run
# in another shell:
curl http://localhost:8080/health

# Integration test against that running instance
SERVICE_BASE_URL=http://localhost:8080 ./gradlew integrationTest
```

## Running via Docker / Compose

```bash
# Build the image
docker build -t distributed-priority-queue-service:local .

# Bring up "staging" (host port 8080) or "prod" (host port 8081) — both
# profiles run the exact same image, parameterized by IMAGE_TAG
IMAGE_TAG=distributed-priority-queue-service:local docker compose --profile staging up -d
curl http://localhost:8080/health

IMAGE_TAG=distributed-priority-queue-service:local docker compose --profile prod up -d
curl http://localhost:8081/health

docker compose --profile staging down
docker compose --profile prod down
```

## CI/CD pipeline (`.github/workflows/ci.yml`)

```
build → deploy-staging → integration-tests-staging → deploy-prod
```

**Build once, promote the same artifact.** `build` compiles, runs unit
tests, builds exactly one Docker image, tags it with the git SHA
(`ghcr.io/<repo>:<sha>`), and pushes it to GHCR. Every later stage
references that same `IMAGE_TAG` — nothing is rebuilt or re-tagged after
this point. "Promotion to prod" means running that identical, already-tested
artifact, not producing a new build from the same source a second time.

**The integration-test gate.** `deploy-prod` declares
`needs: [build, integration-tests-staging]`. If the integration-test job
fails (or is skipped), GitHub Actions skips `deploy-prod` outright — there is
no path from a red pipeline to a prod deployment.

**Honest note on staging/prod.** `deploy-staging`,
`integration-tests-staging`, and `deploy-prod` each spin up the promoted
image as an **ephemeral `docker compose` container on the GitHub Actions
runner itself**, and tear it down at the end of the job. This is not a real,
persistent staging or prod host — there isn't one for this take-home. It's
worth calling out explicitly because of a real GitHub Actions constraint:
each job in a workflow runs on its own isolated, disposable VM, so a
container started in `deploy-staging` cannot be reached from
`integration-tests-staging` (a different job, a different machine).
`integration-tests-staging` therefore deploys its own fresh instance of the
*same* image before testing it — the artifact never changes, only the
disposable container running it does. If/when this pipeline targets real
infrastructure, `deploy-staging`/`deploy-prod` would instead push the image
to an actual long-lived environment (e.g. `kubectl set image`, an ECS
deployment, SSH + compose on a real host), and
`integration-tests-staging` would point `SERVICE_BASE_URL` at that
environment's real address instead of `localhost`.
