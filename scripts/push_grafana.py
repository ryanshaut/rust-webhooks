#!/usr/bin/env python3
"""Push Grafana dashboards from a local directory to a Grafana instance.

This script imports every .json dashboard file it finds under a directory,
using either the legacy /api/dashboards/db endpoint or the v2 dashboard API.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse
from urllib import error, request
from dotenv import load_dotenv

load_dotenv()


DEFAULT_DASHBOARDS_DIR = "dashboards/grafana"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_GRAFANA_NAMESPACE = "default"


def _to_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_grafana_url(raw_url: str) -> tuple[str, bool]:
    """Return API base URL and whether the input looked like a dashboard URL."""
    parsed = urlparse(raw_url.strip())
    path = parsed.path.rstrip("/")
    looked_like_dashboard_url = False

    for marker in ("/d/", "/dashboards/"):
        if marker in path:
            path = path.split(marker, 1)[0]
            looked_like_dashboard_url = True
            break

    normalized = parsed._replace(path=path, params="", query="", fragment="")
    return urlunparse(normalized).rstrip("/"), looked_like_dashboard_url


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Push dashboard JSON files to Grafana."
    )
    parser.add_argument(
        "--grafana-url",
        default=os.getenv("GRAFANA_URL"),
        help="Grafana base URL, e.g. https://grafana.example.com",
    )
    parser.add_argument(
        "--grafana-token",
        default=os.getenv("GRAFANA_TOKEN"),
        help="Grafana service-account token with dashboard write permissions",
    )
    parser.add_argument(
        "--dashboards-dir",
        default=os.getenv("GRAFANA_DASHBOARDS_DIR", DEFAULT_DASHBOARDS_DIR),
        help="Directory containing dashboard JSON files",
    )
    parser.add_argument(
        "--folder-uid",
        default=os.getenv("GRAFANA_FOLDER_UID"),
        help="Grafana folder UID to import dashboards into",
    )
    parser.add_argument(
        "--folder-id",
        type=int,
        default=None,
        help="Legacy Grafana folder numeric ID (prefer --folder-uid/GRAFANA_FOLDER_UID)",
    )
    parser.add_argument(
        "--namespace",
        default=os.getenv("GRAFANA_NAMESPACE", DEFAULT_GRAFANA_NAMESPACE),
        help="Grafana API namespace for v2 dashboards (default: default)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=_to_bool(os.getenv("GRAFANA_OVERWRITE"), True),
        help="Overwrite existing dashboard with same UID (default: true)",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_false",
        dest="overwrite",
        help="Do not overwrite existing dashboards",
    )
    parser.add_argument(
        "--verify-tls",
        action="store_true",
        default=_to_bool(os.getenv("GRAFANA_VERIFY_TLS"), True),
        help="Verify TLS certificates (default: true)",
    )
    parser.add_argument(
        "--insecure",
        action="store_false",
        dest="verify_tls",
        help="Disable TLS certificate verification",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.getenv("GRAFANA_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)),
        help="HTTP timeout in seconds (default: 30)",
    )
    return parser


def _load_dashboard(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Dashboard file must contain a JSON object: {path}")
    return data


def _slugify_name(value: str) -> str:
    slug = re.sub(r"[^a-z0-9-]+", "-", value.strip().lower())
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or "dashboard"


def _is_v2_dashboard(dashboard: dict[str, Any]) -> bool:
    if "spec" in dashboard and isinstance(dashboard["spec"], dict):
        return True
    if str(dashboard.get("apiVersion", "")).startswith("dashboard.grafana.app/"):
        return True
    # Grafana v2 dashboard JSON export shape.
    return all(
        key in dashboard for key in ("elements", "layout", "timeSettings", "title")
    )


def _request_json(
    *,
    url: str,
    grafana_token: str,
    payload: dict[str, Any],
    method: str,
    timeout_seconds: int,
    verify_tls: bool,
) -> tuple[int, dict[str, Any]]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url=url,
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {grafana_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    if verify_tls:
        context = None
    else:
        import ssl

        context = ssl._create_unverified_context()

    with request.urlopen(req, timeout=timeout_seconds, context=context) as response:
        status_code = response.getcode()
        raw = response.read().decode("utf-8")
        return status_code, json.loads(raw) if raw else {}


def _build_v2_resource(
    *,
    dashboard: dict[str, Any],
    dashboard_file: Path,
    folder_uid: str | None,
) -> dict[str, Any]:
    api_version = str(dashboard.get("apiVersion") or "dashboard.grafana.app/v2")
    if api_version == "dashboard.grafana.app/v1":
        api_version = "dashboard.grafana.app/v2"
    kind = str(dashboard.get("kind") or "Dashboard")

    if "spec" in dashboard and isinstance(dashboard["spec"], dict):
        spec = dict(dashboard["spec"])
    else:
        spec = dict(dashboard)

    metadata_in = dashboard.get("metadata")
    metadata = dict(metadata_in) if isinstance(metadata_in, dict) else {}
    annotations_in = metadata.get("annotations")
    annotations = dict(annotations_in) if isinstance(annotations_in, dict) else {}

    if folder_uid:
        annotations["grafana.app/folder"] = folder_uid
    if annotations:
        metadata["annotations"] = annotations

    name = metadata.get("name")
    if not isinstance(name, str) or not name.strip():
        uid_candidate = spec.get("uid")
        if isinstance(uid_candidate, str) and uid_candidate.strip():
            name = uid_candidate
        else:
            title_candidate = spec.get("title") or dashboard.get("title") or dashboard_file.stem
            name = _slugify_name(str(title_candidate))
    metadata["name"] = name

    return {
        "apiVersion": api_version,
        "kind": kind,
        "metadata": metadata,
        "spec": spec,
    }


def _post_dashboard(
    *,
    grafana_url: str,
    grafana_token: str,
    dashboard: dict[str, Any],
    overwrite: bool,
    timeout_seconds: int,
    verify_tls: bool,
    folder_uid: str | None,
    folder_id: int | None,
) -> tuple[int, dict[str, Any]]:
    payload: dict[str, Any] = {
        "dashboard": dashboard,
        "overwrite": overwrite,
    }
    if folder_uid:
        payload["folderUid"] = folder_uid
    elif folder_id is not None:
        payload["folderId"] = folder_id

    return _request_json(
        url=f"{grafana_url.rstrip('/')}/api/dashboards/db",
        grafana_token=grafana_token,
        payload=payload,
        method="POST",
        timeout_seconds=timeout_seconds,
        verify_tls=verify_tls,
    )


def _upsert_dashboard_v2(
    *,
    grafana_url: str,
    grafana_token: str,
    dashboard: dict[str, Any],
    dashboard_file: Path,
    overwrite: bool,
    timeout_seconds: int,
    verify_tls: bool,
    folder_uid: str | None,
    namespace: str,
) -> tuple[int, dict[str, Any], str]:
    resource = _build_v2_resource(
        dashboard=dashboard,
        dashboard_file=dashboard_file,
        folder_uid=folder_uid,
    )

    name = str(resource["metadata"]["name"])
    base_v2 = (
        f"{grafana_url.rstrip('/')}/apis/dashboard.grafana.app/v2/"
        f"namespaces/{namespace}/dashboards"
    )
    base_v1 = (
        f"{grafana_url.rstrip('/')}/apis/dashboard.grafana.app/v1/"
        f"namespaces/{namespace}/dashboards"
    )

    if overwrite:
        for base in (base_v2, base_v1):
            try:
                status_code, response_json = _request_json(
                    url=f"{base}/{name}",
                    grafana_token=grafana_token,
                    payload=resource,
                    method="PUT",
                    timeout_seconds=timeout_seconds,
                    verify_tls=verify_tls,
                )
                return status_code, response_json, "PUT"
            except error.HTTPError as exc:
                if exc.code == 404:
                    continue
                raise

    last_404: error.HTTPError | None = None
    for base in (base_v2, base_v1):
        try:
            status_code, response_json = _request_json(
                url=base,
                grafana_token=grafana_token,
                payload=resource,
                method="POST",
                timeout_seconds=timeout_seconds,
                verify_tls=verify_tls,
            )
            return status_code, response_json, "POST"
        except error.HTTPError as exc:
            if exc.code == 404:
                last_404 = exc
                continue
            raise

    if last_404 is not None:
        raise last_404
    raise RuntimeError("Unable to reach Grafana dashboard API endpoints")


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    folder_id = args.folder_id
    if folder_id is None and not args.folder_uid:
        folder_id_raw = os.getenv("GRAFANA_FOLDER_ID")
        if folder_id_raw:
            try:
                folder_id = int(folder_id_raw)
            except ValueError:
                parser.error(
                    "GRAFANA_FOLDER_ID must be an integer. "
                    "Use GRAFANA_FOLDER_UID for string UIDs."
                )

    if not args.grafana_url:
        parser.error("Missing Grafana URL. Set --grafana-url or GRAFANA_URL.")
    if not args.grafana_token:
        parser.error("Missing Grafana token. Set --grafana-token or GRAFANA_TOKEN.")

    grafana_url, normalized_from_dashboard = _normalize_grafana_url(args.grafana_url)
    if normalized_from_dashboard:
        print(
            "INFO normalized GRAFANA_URL from dashboard URL to API base: "
            f"{grafana_url}",
            file=sys.stderr,
        )

    dashboards_dir = Path(args.dashboards_dir)
    if not dashboards_dir.exists():
        parser.error(f"Dashboard directory does not exist: {dashboards_dir}")

    dashboard_files = sorted(dashboards_dir.rglob("*.json"))
    if not dashboard_files:
        print(f"No dashboard JSON files found in {dashboards_dir}")
        return 0

    failures = 0
    for dashboard_file in dashboard_files:
        uid = "<unknown>"
        try:
            dashboard = _load_dashboard(dashboard_file)
            used_api = "legacy"
            used_method = "POST"

            if _is_v2_dashboard(dashboard):
                if folder_id is not None and not args.folder_uid:
                    print(
                        "WARN folder-id is ignored for v2 dashboards; "
                        "use --folder-uid/GRAFANA_FOLDER_UID",
                        file=sys.stderr,
                    )

                title = dashboard.get("title") or dashboard.get("spec", {}).get(
                    "title", dashboard_file.stem
                )
                uid = dashboard.get("uid") or dashboard.get("spec", {}).get(
                    "uid", "<no-uid>"
                )
                status_code, response_json, used_method = _upsert_dashboard_v2(
                    grafana_url=grafana_url,
                    grafana_token=args.grafana_token,
                    dashboard=dashboard,
                    dashboard_file=dashboard_file,
                    overwrite=args.overwrite,
                    timeout_seconds=args.timeout_seconds,
                    verify_tls=args.verify_tls,
                    folder_uid=args.folder_uid,
                    namespace=args.namespace,
                )
                used_api = "v2"
            else:
                # Avoid importing stale numeric IDs from exported legacy JSON.
                dashboard["id"] = None
                title = dashboard.get("title", dashboard_file.stem)
                uid = dashboard.get("uid", "<no-uid>")

                status_code, response_json = _post_dashboard(
                    grafana_url=grafana_url,
                    grafana_token=args.grafana_token,
                    dashboard=dashboard,
                    overwrite=args.overwrite,
                    timeout_seconds=args.timeout_seconds,
                    verify_tls=args.verify_tls,
                    folder_uid=args.folder_uid,
                    folder_id=folder_id,
                )

            status = response_json.get("status", "unknown")
            url = response_json.get("url", "")
            version = response_json.get("version", "?")
            if used_api == "v2":
                spec = response_json.get("spec") if isinstance(response_json, dict) else {}
                metadata = (
                    response_json.get("metadata") if isinstance(response_json, dict) else {}
                )
                if isinstance(spec, dict):
                    version = spec.get("version", version)
                    url = spec.get("url", url)
                if isinstance(metadata, dict) and metadata.get("name"):
                    uid = str(metadata.get("name"))
            print(
                f"OK   {dashboard_file} | title='{title}' uid='{uid}' "
                f"api={used_api}:{used_method} "
                f"status={status_code}/{status} version={version} url={url}"
            )
        except ValueError as exc:
            failures += 1
            print(f"FAIL {dashboard_file} | invalid JSON: {exc}", file=sys.stderr)
        except error.HTTPError as exc:
            failures += 1
            details = exc.read().decode("utf-8", errors="replace")
            hint = ""
            if exc.code == 403:
                hint = (
                    " Hint: token may be missing access to the target folder or "
                    f"existing dashboard UID '{uid}'. If overwrite is enabled, "
                    "the token needs read/write access to that existing dashboard."
                )
            print(
                f"FAIL {dashboard_file} | HTTP {exc.code}: {details}{hint}",
                file=sys.stderr,
            )
        except error.URLError as exc:
            failures += 1
            print(f"FAIL {dashboard_file} | URL error: {exc.reason}", file=sys.stderr)
        except json.JSONDecodeError as exc:
            failures += 1
            print(f"FAIL {dashboard_file} | JSON parse error: {exc}", file=sys.stderr)
        except Exception as exc:  # pragma: no cover
            failures += 1
            print(f"FAIL {dashboard_file} | unexpected error: {exc}", file=sys.stderr)

    if failures:
        print(f"Completed with {failures} failure(s).", file=sys.stderr)
        return 1

    print(f"Successfully pushed {len(dashboard_files)} dashboard(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())