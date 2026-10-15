from unittest.mock import Mock

import pytest

from gridtime.config import Settings
from gridtime.quality import scan as module


@pytest.mark.parametrize(
    "code,checks",
    [
        (0, []),
        (0, [{"outcome": "fail"}]),
        (1, [{"outcome": "warn"}]),
        (3, [{"outcome": "pass"}]),
    ],
)
def test_scan_errors_empty_scans_and_nonpasses_fail_closed(tmp_path, monkeypatch, code, checks):
    scanner = Mock()
    scanner.execute.return_value = code
    scanner.get_scan_results.return_value = {"checks": checks}
    monkeypatch.setattr(module, "Scan", Mock(return_value=scanner))
    monkeypatch.setattr(module, "get_settings", lambda: Settings(data_dir=str(tmp_path)))
    with pytest.raises(module.QualityError):
        module.scan_layer("silver", "lakehouse_wap")
    assert list((tmp_path / "quality").glob("*.json"))
