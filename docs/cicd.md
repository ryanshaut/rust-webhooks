# Release container images

This guide describes the workflows actually checked into the repository:

- **Container** (`container.yml`): builds pull requests and publishes images on
  configured branch/tag pushes.
- **Documentation** (`docs.yml`): validates and publishes the static documentation
  site. See [Publish documentation](how-to/publish-docs.md).

There is currently no separate `ci.yml`, `security.yml`, or application `deploy.yml`.
Do not rely on checks or deployments from those workflows.

## Check the application before releasing

Run the [local Rust checks and integration tests](how-to/test.md). The Container
workflow builds the release binary as part of the Docker build, but it does not
run the formatting, Clippy, or test suite.

Prefer short-lived `feature/*` branches, pull requests into `main`, and squash
merges. Configure branch protection against checks that actually exist, such as
Container's `build-image` and Documentation's `Build documentation` when applicable.
Documentation checks are path-filtered; requiring them for every pull request
would leave unrelated pull requests waiting for a skipped workflow.

## Publish from a branch

Pushes to `main` and `feature/**` trigger the container workflow. Pull requests
build images but do not log in to GHCR or push them.

Published images use `ghcr.io/ryanshaut/rust-webhooks`:

| Trigger | Tags |
| --- | --- |
| Push to `main` | `latest` and `sha-<short-sha>` |
| Push to `feature/*` | Sanitized branch tag, such as `feature-example`, and SHA tag |
| Matching semantic-version tag | Full version without leading `v`, major/minor version, and SHA tag |

For example, `v1.2.3` produces `1.2.3` and `1.2`, not an image tag named `v1.2.3`.

## Publish a version

After reviewing and merging a tested commit:

```bash
git tag v1.2.3
git push origin v1.2.3
```

The tag pattern `v*.*.*` triggers version publishing. The workflow uses the
repository's `GITHUB_TOKEN` with package write permission; no personal token
is required.

Publishing an image does not deploy it. Choose and configure your own application
hosting environment; the only deployment workflow provided here is for GitHub Pages.
