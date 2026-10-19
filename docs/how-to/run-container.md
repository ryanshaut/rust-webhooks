# Run the published container

Use this guide when you want to run the API without a local Rust toolchain.
You need Docker and a reachable PostgreSQL database.

## Run against PostgreSQL on your host

Start the development database if needed:

```bash
docker compose -f db.docker-compose.yml up -d db
```

On Linux, add the host gateway mapping; Docker Desktop also supports
`host.docker.internal`:

```bash
docker run --rm -p 3001:3000 \
  --add-host=host.docker.internal:host-gateway \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD= \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_DATABASE=rust-webhooks \
  ghcr.io/ryanshaut/rust-webhooks:latest
```

The API listens inside the container on port 3000 and is available on the host at
`http://localhost:3001`. The schema is initialized on startup.

For a managed database, replace the database environment variables with the values
provided by your administrator. Prefer a version or SHA tag over `latest` for a
repeatable deployment; see [image publishing](../cicd.md).

## Protect the deployment

- Put authentication and TLS termination in a reverse proxy or trusted ingress.
- Support WebSocket upgrades if you use WebSocket consumers.
- Use private database networking and database authentication.
- Do not reuse the development Compose trust-authentication settings in production.
- Keep producers and WebSocket consumers on the same instance; see
  [delivery limitations](../explanation/lifecycle.md#websocket-delivery-and-replicas).

The repository does not contain an application deployment workflow or
production-ready infrastructure configuration. The GitHub Pages workflow deploys
only static documentation.
