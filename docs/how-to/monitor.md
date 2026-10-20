# Monitor webhooks with Grafana

Use the bundled dashboards to inspect traffic, dimensions, payloads, and lifecycle state.

## Start the monitoring stack

```bash
docker compose -f db.docker-compose.yml up -d
```

The Compose file starts PostgreSQL on port 5432, Adminer on 8080, and Grafana on
3000. Run the API on a different host port:

```bash
PORT=3001 cargo run
```

Use the API's default development database (`rust-webhooks`) so the provisioned
Grafana datasource sees the same data.

Open `http://localhost:3000` and use Grafana's default local credentials
(`admin` / `admin`, unless changed). Do not expose these defaults outside a trusted
development environment.

## Select a dashboard

| Dashboard | Purpose |
| --- | --- |
| Webhook Overview | Throughput, top tenants/apps, event mix, and recent payload explorer |
| Webhook Dimensions Catalog | Distinct tenants, apps, events, and their combinations |
| Webhook Payload Detail | Request metadata, headers, query parameters, and body for one ID |
| Callback URLs Overview | One-off callback totals, pending/active/fulfilled/expired counts, ages, and time to fulfill |

Provisioning loads dashboard JSON from
[grafana/dashboards](https://github.com/ryanshaut/rust-webhooks/tree/main/grafana/dashboards)
and the datasource from
[grafana/provisioning](https://github.com/ryanshaut/rust-webhooks/tree/main/grafana/provisioning).

Send requests with paths shaped like `/api/webhooks/{tenant}/{app}/{event}/...`.
The dashboards derive these dimensions from the first three path segments after
`/api/webhooks`, not from headers or payload properties.

Select a dimension value or `All`. Use `payload_search` in the overview to filter
the request path or UTF-8 body text.

## Push dashboards to another Grafana instance

From the repository root, install the example dependencies with `uv sync`.
Provide a Grafana service-account token with dashboard write permission through
your local environment, then run:

```bash
export GRAFANA_URL=https://grafana.example.com
# Set GRAFANA_TOKEN securely in your environment.
uv run python scripts/push_grafana.py --dashboards-dir grafana/dashboards
```

Specify the directory explicitly: the script's default directory is not the
repository's provisioned dashboard directory. Inspect other options with `--help`.
