"""Register existing metadata without copying data. Run with all writers stopped."""

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from gridtime.config import get_settings
from gridtime.storage.catalog import nessie_catalog, sql_catalog
from gridtime.storage.tables import BRONZE_TABLE, SILVER_TABLE


def migrate(*, allow_appended_destination: bool = False) -> dict:
    s = get_settings()
    path = Path(s.catalog_uri.removeprefix("sqlite:///"))
    if not s.catalog_uri.startswith("sqlite:///") or not path.is_file():
        raise ValueError("Migration requires an existing SQLite catalog; stop writers first")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    backup = path.with_name(f"{path.name}.backup-{stamp}")
    with sqlite3.connect(path) as source, sqlite3.connect(backup) as dest:
        source.backup(dest)
    old, new = sql_catalog(), nessie_catalog()
    report = {"backup": str(backup), "verified_at": stamp, "tables": {}}
    for name in (BRONZE_TABLE, SILVER_TABLE):
        original = old.load_table(name)
        new.create_namespace_if_not_exists(name.split(".")[0])
        if not new.table_exists(name):
            new.register_table(name, original.metadata_location)
        registered = new.load_table(name)
        # Refuse to overwrite a diverged destination on a rerun.
        diverged = registered.metadata_location != original.metadata_location
        if diverged and not allow_appended_destination:
            raise RuntimeError(f"Destination has diverged: {name}; refusing to replace it")
        old_files = {task.file.file_path for task in original.scan().plan_files()}
        new_files = {task.file.file_path for task in registered.scan().plan_files()}
        before = original.scan().count()
        after = registered.scan().count()
        if (
            (before != after if not diverged else before > after or not old_files <= new_files)
            or original.metadata.table_uuid != registered.metadata.table_uuid
            or original.schema() != registered.schema()
            or (
                not diverged
                and original.metadata.current_snapshot_id
                != registered.metadata.current_snapshot_id
            )
        ):
            raise RuntimeError(f"Catalog verification failed for {name}")
        report["tables"][name] = {
            "source_rows": before,
            "rows": after,
            "base_files_preserved": old_files <= new_files,
            "metadata": registered.metadata_location,
        }
    s.data_path.mkdir(parents=True, exist_ok=True)
    (s.data_path / "catalog-migration.json").write_text(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    print(json.dumps(migrate(), indent=2))
