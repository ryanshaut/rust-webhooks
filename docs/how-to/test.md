# Run tests and load tests

## Run Rust checks

From the repository root:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
cargo build --release --locked
```

`cargo test` runs database-independent unit/routing tests by default. PostgreSQL
integration tests are marked ignored. These commands are local checks; the
repository does not currently have a separate Rust CI workflow.

## Include PostgreSQL integration tests

Use a dedicated, disposable database, not a production or shared database.

```bash
docker compose -f db.docker-compose.yml up -d db
docker exec postgres pg_isready -U postgres
docker exec postgres createdb -U postgres rust-webhooks-test
TEST_DATABASE_URL=postgres://postgres@localhost:5432/rust-webhooks-test \
  cargo test -- --include-ignored
```

Wait for PostgreSQL to accept connections before creating the database. If the
test database already exists, omit `createdb`. Adapt the host and port if you use a
different PostgreSQL instance.

The integration tests exercise callback fulfillment/expiration, WebSocket
delivery, and the HTTP capture/consumer/operator lifecycle. They create schema
and modify data. Within one test process they serialize database integration tests
to avoid concurrent schema creation.

For narrower checks:

```bash
cargo test websocket
TEST_DATABASE_URL=postgres://postgres@localhost:5432/rust-webhooks-test \
  cargo test websocket_delivers -- --ignored
```

Passing tests do not establish 100% line or branch coverage; there is no automated
coverage threshold configured.

## Load-test a running API

The k6 targets run the standard Grafana k6 Docker image. They require Docker,
not a local k6 installation. Point them explicitly at your running API:

```bash
BASE_URL=http://127.0.0.1:3001 VUS=50 DURATION=60s RUN_ID=local make k6
BASE_URL=http://127.0.0.1:3001 make k6-smoke
```

`make k6` defaults to 10 virtual users for 30 seconds; `make k6-smoke` uses
100 users for 60 seconds. The targets use host networking, so the commands assume
a Docker environment supporting that mode.

Load tests create stored requests and consumer activity. Run them only against
an environment intended for testing.
