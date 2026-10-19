# rust-webhooks documentation

Capture inbound HTTP requests in PostgreSQL, let consumers claim and complete them,
and inspect delivery state through operator APIs and Grafana.

This documentation follows [Diataxis](https://diataxis.fr/): choose a section based
on whether you want to learn, accomplish a task, look up a fact, or understand a design.

| Your goal | Start here |
| --- | --- |
| Learn by doing | [Tutorial: your first webhook](tutorials/first-webhook.md) |
| Stream webhooks to a consumer | [Tutorial: receive over WebSocket](tutorials/websocket.md) |
| Complete a specific task | [Run a container](how-to/run-container.md), [monitor with Grafana](how-to/monitor.md), or [run tests](how-to/test.md) |
| Develop and maintain the project | [Test the application](how-to/test.md), [release images](cicd.md), or [publish documentation](how-to/publish-docs.md) |
| Look up exact behavior | [Configuration](reference/configuration.md), [HTTP API](reference/http-api.md), [WebSocket API](reference/websocket-api.md), and [storage](reference/storage.md) |
| Understand the system | [Architecture](explanation/architecture.md) and [delivery lifecycle](explanation/lifecycle.md) |

!!! warning "Private-network application"
    The API has no built-in authentication or authorization. Use trusted networks
    or an authenticated reverse proxy. Sensitive header/query redaction is not
    payload encryption, webhook signature verification, or an access-control layer.

Source and container images:

- [GitHub repository](https://github.com/ryanshaut/rust-webhooks)
- Container image: `ghcr.io/ryanshaut/rust-webhooks`
