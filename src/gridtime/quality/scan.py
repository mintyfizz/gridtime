"""Fail closed: errors, warnings, empty scans and failed checks all block WAP."""

import json
from datetime import UTC, datetime

import yaml
from soda.scan import Scan

from gridtime.config import REPO_ROOT, get_settings


class QualityError(RuntimeError):
    pass


def scan_layer(layer: str, catalog: str = "lakehouse", *, require_pass: bool = True) -> dict:
    if layer not in {"bronze", "silver", "gold"}:
        raise ValueError(layer)
    s = get_settings()
    scan = Scan()
    scan.disable_telemetry()
    scan.set_scan_definition_name(f"gridtime-{catalog}-{layer}")
    scan.set_data_source_name("gridtime")
    scan.add_configuration_yaml_str(
        yaml.safe_dump(
            {
                "data_source gridtime": {
                    "type": "trino",
                    "host": s.trino_host,
                    "port": s.trino_port,
                    "username": s.trino_user,
                    "http_scheme": "http",
                    "auth_type": "NoAuthentication",
                    "catalog": catalog,
                    "schema": layer,
                }
            }
        )
    )
    scan.add_sodacl_yaml_file(str(REPO_ROOT / "quality" / f"{layer}.yml"))
    code = scan.execute()
    results = scan.get_scan_results()
    checks = results.get("checks", [])
    passed = code == 0 and bool(checks) and all(c.get("outcome") == "pass" for c in checks)
    report = {
        "passed": passed,
        "exit_code": code,
        "layer": layer,
        "catalog": catalog,
        "checks": checks,
        "logs": results.get("logs", []),
    }
    directory = s.data_path / "quality"
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    path = directory / f"{stamp}-{catalog}-{layer}.json"
    path.write_text(json.dumps(report, indent=2, default=str))
    report["report_path"] = str(path)
    if require_pass and not passed:
        raise QualityError(f"{layer} Soda audit failed; see {path}")
    return report


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("layer", choices=["bronze", "silver", "gold"])
    parser.add_argument("--catalog", default="lakehouse")
    args = parser.parse_args()
    result = scan_layer(args.layer, args.catalog)
    print(f"{args.layer}: {len(result['checks'])} checks passed")
