# Build and publish the documentation

The site uses MkDocs with Material for MkDocs. GitHub Actions builds static HTML
and deploys it with the official GitHub Pages artifact/deployment actions. No
documentation server or third-party hosting account is required.

## Preview locally

Use Python 3.11 or newer (the workflow uses 3.12). From
the repository root:

```bash
python3 -m venv .venv-docs
.venv-docs/bin/python -m pip install -r docs/requirements.txt
.venv-docs/bin/python -m mkdocs serve --dev-addr 127.0.0.1:8000
```

Open `http://127.0.0.1:8000/rust-webhooks/`. Stop the preview with Ctrl+C.
The isolated documentation environment does not change the application's Python
example dependencies.

On systems without Python's `venv`/`ensurepip` support, install the operating
system's Python venv package or use uv instead:

```bash
uv venv .venv-docs
uv pip install --python .venv-docs/bin/python -r docs/requirements.txt
.venv-docs/bin/python -m mkdocs serve --dev-addr 127.0.0.1:8000
```

For a production build:

```bash
.venv-docs/bin/python -m mkdocs build --strict
```

HTML is generated in `site/`, which is ignored by Git. The strict build rejects
warnings, including missing navigation targets, missing local documents, and
broken Markdown heading anchors. It does not check external URL availability.

## Enable GitHub Pages once

A repository administrator must:

1. Open the repository's **Settings > Pages**.
2. Set **Build and deployment > Source** to **GitHub Actions**.
3. Ensure GitHub Actions is enabled and the `github-pages` environment permits
   deployments from `main`. Add required reviewers if desired.
4. Merge the documentation workflow into `main`, or select **Actions >
   Documentation > Run workflow** on `main`.

The expected site URL is
<https://ryanshaut.github.io/rust-webhooks/>. It will not become available until
the workflow is committed, pushed, and completes successfully with Pages enabled.
No personal access token or repository secret is needed; deployment uses the
workflow's `GITHUB_TOKEN` and OIDC identity.

## Understand workflow behavior

- Documentation-related pull requests build and validate the site without
  deploying it or granting Pages write permissions.
- Documentation-related pushes to `main` build the site and deploy the generated
  artifact to the `github-pages` environment.
- Manual runs on `main` also deploy. Manual runs on other branches only build.
- A failed build prevents deployment. Main-branch runs are not cancelled mid-deployment;
  newer pull-request runs can replace older runs for the same ref.

The workflow is
[docs.yml](https://github.com/ryanshaut/rust-webhooks/blob/main/.github/workflows/docs.yml).
It does not publish branch previews or deploy the Rust application.

## Add or update a page

1. Choose the appropriate [Diataxis](https://diataxis.fr/) section:
   tutorial for a guided learning exercise, how-to for a specific task, reference
   for factual contracts, or explanation for design and tradeoffs.
2. Add or edit Markdown under `docs/`. Use relative links between pages.
3. Add new pages to the `nav` section in `mkdocs.yml`.
4. Build with `--strict`, inspect the local preview, and open a pull request.

Keep the README as a short entry point rather than duplicating detailed site content.
Update the pinned documentation dependencies in `docs/requirements.txt` separately
from the Python example dependencies.
