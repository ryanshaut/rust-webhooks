# CI/CD guide

## Workflow overview

This repository uses four GitHub Actions workflows:

- `ci.yml`: runs formatting, clippy, tests, and release builds for pull requests and pushes to `main` / `feature/**`.
- `container.yml`: builds and publishes images to GHCR with branch/SHA/version tagging.
- `security.yml`: runs `cargo audit` plus Trivy scans (filesystem and image), and uploads SARIF.
- `deploy.yml`: manual deployment scaffold for `staging` and `production` environments.

## Branch strategy

- `main`: protected integration branch.
- `feature/*`: short-lived feature branches.
- Pull requests required before merging to `main`.
- CI must pass before merge.
- Squash merge preferred.

### Sample branch protection (main)

Suggested settings in GitHub branch protection:

- Require a pull request before merging
- Require approvals (for example: at least 1)
- Require status checks to pass before merging (`CI`, `Container`, and `Security` as desired)
- Require branches to be up to date before merging
- Restrict force pushes and branch deletion
- Enforce for administrators

## Image naming conventions

Images are pushed to:

- `ghcr.io/<owner>/<repo>`

Tagging rules:

- `main` branch push: `latest`
- `feature/*` branch push: `feature-<branch-name>`
- any push/tag build: `sha-<short-sha>`
- semantic version tags `vX.Y.Z`: `vX.Y.Z` and `X.Y`

## Run checks locally

Use the same commands as CI:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
cargo build --release --locked
```

## Create a release tag

```bash
git tag v1.0.0
git push origin v1.0.0
```

This triggers versioned container publishing.

## Manually trigger deployment

1. Open **Actions** in GitHub.
2. Select the **Deploy** workflow.
3. Click **Run workflow**.
4. Choose `staging` or `production`.

`production` should use GitHub Environment protection rules (for example, required reviewers) before job execution.
